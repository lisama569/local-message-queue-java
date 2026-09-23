# 本地消息队列（Local Message Queue, Java / Spring Boot）

一套**完全本地、零外部依赖**的消息消费 / 确认 / 重试编排能力：不需要 Kafka、数据库、
Redis 或任何网络服务 / 真实凭据，只用本地文件（JSON Lines + 原子状态文件）持久化。
适用于本地开发、集成测试、教学演示，以及对「确定性可解释」要求高的嵌入式场景。

- 主题（topic）、分区（partition）、消费者组（consumer group）语义
- 同分区严格有序、不同分区并发消费
- at-least-once 投递 + 消息标识去重账本 ⇒ 「不丢失、不重复产生副作用」
- 失败三分类（可重试 / 不可重试 / 重试耗尽）、指数退避、死信队列（DLQ）
- 死信重放复用原消息标识，**不绕过**重复消费约束
- 优雅关停（拒绝新任务、限时排空）与 kill -9 硬崩溃恢复
- 明确可区分的容量拒绝与关停拒绝

---

## 1. 快速开始

```java
BrokerConfig config = BrokerConfig.fast(); // 退避20ms / 最多3次 / 积压16 / 关停等5s / 处理超时2s

LocalMessageBroker broker = new LocalMessageBroker(
        Path.of("data/lmq"), "orders", /*partitions*/ 2, config,
        message -> {
            // 业务处理：返回 ack() / retry() / deadLetter()；抛异常默认可重试
            return HandleResult.ack();
        });
broker.start();

Message m = broker.send("""{"orderId":42}""");          // 轮询分区，自动生成 id
broker.sendToPartition(0, "payload", "msg-123");        // 指定分区 / 指定 messageId

broker.awaitDrained(5_000);                             // 等待全部到达终态
List<DeadLetterRecord> dead = broker.deadLetters();     // 查看死信
broker.replayDeadLetter("msg-123");                     // 重放某条死信
broker.shutdown();                                      // 优雅关停（也可 try-with-resources）
```

重启恢复：**用同一个数据目录重新构造并启动 broker** 即可。这在语义上等价于「同一个
消费者组在成员重启 / 增减后重新接管全部分区」；位点、去重账本、待重试态、死信全部
来自磁盘，未确认消息会从原位点重新投递。

---

## 2. 磁盘布局与数据模型

数据目录（默认自行创建）下，每个分区 3 个文件，外加一个 broker 级死信日志：

```
<dataDir>/
├── <topic>-<p>.log        # 分区消息日志，JSON Lines 追加（行号即位点 offset，从0开始）
├── <topic>-<p>.ledger     # 去重账本，JSON Lines 追加（COMPLETED / DEAD_LETTERED）
├── <topic>-<p>.state      # 可变状态：committedOffset + pendingRetry + 重放终态位点
└── dead-letters.log       # 死信日志，JSON Lines 追加，保留原始载荷与失败原因
```

- **消息日志 `*.log`**：每行 `{"id","payload","replay"}`，追加后 `fsync`。
  末尾写了一半的行在重启重放时被忽略（该次 append 从未对生产者返回成功）。
- **去重账本 `*.ledger`**：只追加、`fsync`。记录两类事实：
  - `COMPLETED`：消息业务处理成功（已产生副作用并确认）；
  - `DEAD_LETTERED`：消息已进入死信。
- **状态文件 `*.state`**：通过「写临时文件 → fsync → 原子 rename（必要时目录 fsync）」
  整体替换，任何时刻磁盘上要么是旧状态要么是新状态，不会出现半个文件。
- **消息标识 `id`**：全局唯一，是去重与重放的唯一依据；位点 `offset` 仅在分区内有意义。

---

## 3. 交付语义（Delivery Semantics）

**整体为「at-least-once 投递 + 幂等去重」，对业务副作用呈现 effectively-once（不重复）。**

### 3.1 分区内顺序与分区间并发

- 每个分区有且仅有一个工作线程串行消费：前一条消息不到达终态（ACK 或 DEAD_LETTER），
  绝不推进到下一条 ⇒ 同分区严格按 offset 顺序处理。
- 不同分区各自独立线程 + 共享的处理器线程池 ⇒ 分区间并发推进。
- 处理器是阻塞调用，由独立线程执行并施加超时，超时不会让分区线程跳到下一条消息。

### 3.2 确认（ACK）与提交的顺序

业务返回 `ack()` 后，在分区线程内、持锁依次完成：

1. 往 `*.ledger` 追加 `COMPLETED(id, offset)` 并 fsync；
2. 原子更新 `*.state` 中的 `committedOffset = offset + 1`。

位点之前的消息视为「全部到达终态」。重启后：

- 已提交位点之前的消息不再投递；
- 位点之后的消息从原位点重新投递；调用 handler 前先查账本：
  - `id` 已 `COMPLETED` / `DEAD_LETTERED` ⇒ **跳过副作用**，仅推进位点；
  - 因此「位点回退导致的重复投递」「同一 id 作为新记录的重复投递」
    「重启后重投」都不会产生第二次副作用。

> 处理器可能被调用多次（可重试失败、重投、重放），**但业务副作用只应发生一次**。
> 测试 `DuplicateDeliveryTest` 明确断言「handler 调用 ≥1 次，副作用计数 == 1」。

### 3.3 已知的最窄重复窗口（恢复边界，必须知悉）

业务副作用（如外部写库）与「完成账本落盘」不是同一个跨资源原子事务。框架在
**业务成功之后**先写账本再提交位点，因此：

| 崩溃发生的位置 | 重启后的结果 |
|---|---|
| 业务副作用执行中 / 之前 | 消息未确认 ⇒ 重投，**不丢失**；副作用尚未发生，无重复 |
| 副作用已完成，但 `COMPLETED` 账本落盘前 | 消息重投、**可能再执行一次副作用**（at-least-once 的固有窄窗口） |
| 账本已落盘，但位点原子替换前 | 重投时被账本判重跳过，**无重复副作用** |
| 位点提交之后 | 不重投 |

若业务本身要求绝对幂等，应在业务侧基于消息 `id` 做幂等键（框架已保证正常重投/重放
路径不重复，仅上表第二行依赖业务幂等）。

---

## 4. 失败分类、退避重试与死信

业务处理器通过 `HandleResult` 表达三种判定：

| 判定 | API | 含义与去向 |
|---|---|---|
| 确认 | `HandleResult.ack()` | 成功 ⇒ 写完成账本 + 提交位点 |
| 可重试失败 | `HandleResult.retry(reason)` | 进入退避重试；达到 `maxAttempts` 后死信，kind=**RETRIES_EXHAUSTED** |
| 不可重试失败 | `HandleResult.deadLetter(reason)` | 首次即死信，kind=**NON_RETRYABLE**，attempts=1 |

- 处理器**直接抛异常**或返回 `null`：按「可重试失败」处理（异常类型进入 reason）。
- 处理**超时**（`handleTimeout`）：取消执行并按「可重试失败」处理。
  超时无法判断副作用是否已发生，故采用 at-least-once 保守策略（可能再执行一次）。
- 退避：第 `n` 次失败后等待 `backoffBase × 2^(n-1)`，上限 30s。
- 每次「判定为重试」都会把 `pendingRetry(offset, attempt, reason, readyAt)`
  **原子持久化到 `*.state`**，然后才开始退避等待——退避期间进程被杀也能恢复，
  重启后沿用 attempt 计数（不会清零导致无限重试）。
- 三类失败可区分：`FailureKind.RETRYABLE`（运行中）、`NON_RETRYABLE`、`RETRIES_EXHAUSTED`；
  死信记录 `DeadLetterRecord` 保留**原始消息（含原始载荷）**、分类、原因、尝试次数、
  进入死信的时间。死信的写入先 fsync 再提交位点，崩溃只会导致重判，不会漏。

---

## 5. 死信重放（Replay）规则

`broker.replayDeadLetter(id)` / `replayAllDeadLetters()`：

1. 从 `dead-letters.log` 找到该 id 的原始消息（主题、分区、位点、**原始载荷**）；
2. 作为**一条新日志记录**追加到**原分区**，获得全新 offset，并置 `replay=true`，
   **复用原消息 id**；
3. 消费到该重放记录时：
   - 若原 id 已在完成账本（历史上曾成功过）⇒ 直接判重跳过，**不产生副作用**；
   - 否则调用 handler：成功则写完成账本；仍失败则按同样的重试 / 死信规则处理。
4. 每条重放记录的「终态」（成功提交 / 再次死信）会以位点集合持久化到 `*.state`，
   重放崩溃重启后该重放记录不会被自动处理第二次。

即：**重放是一次新的投递机会，但绝不绕过「按 id 去重」的重复消费约束。**

---

## 6. 积压控制与优雅关停

### 6.1 容量边界（背压）

`partitionPrefetch` 是每个分区允许的「已落盘未确认」消息数（含正在处理的 1 条 +
排队中）。生产时若 `logEndOffset - committedOffset >= prefetch`，立即抛出：

- **`BacklogCapacityExceededException`** —— 容量拒绝（背压），调用方应稍后重试生产。

### 6.2 优雅关停

`shutdown()`（或 try-with-resources）：

1. 立即「关门」：此后任何 `send` / 重放都抛 **`BrokerShutdownException`**
   （与容量拒绝是不同异常类型，可明确区分）；
2. 通知各分区消费者停止接收新消息，但在 `shutdownAwait` 时限内**继续排空已接收的
   积压**（含到点的重试）——正在处理的消息会跑完，其确认 / 重试 / 死信状态照常落盘；
3. 超过 drain 时限仍未完成的消息保持「未确认」，下次启动从磁盘恢复，不丢；
4. 关闭处理器线程池。

### 6.3 硬崩溃（kill -9 / 掉电）

不执行任何钩子。由于「追加即 fsync、状态原子替换」，重启后：

- 已写完成账本的消息 ⇒ 不重复副作用；
- 未确认 / 处于退避中的消息 ⇒ 原位点重投（attempt 沿用）；
- 排队消息 ⇒ 继续处理；
- 唯一的理论重复窗口见 §3.3。

---

## 7. 验证方法

```bash
mvn test
```

测试位于 `src/test/java/.../tests/`，**不依赖任何外部资源**（临时目录 + 子进程）：

| 测试类 | 覆盖需求 | 关键断言 |
|---|---|---|
| `ConcurrencyOrderingTest` | 并发消费、分区有序 | 两组 300ms 并行完成（总耗时<500ms）；分区内 offset 顺序；每消息副作用==1 |
| `DuplicateDeliveryTest` | 重复投递 | 同 id 两条记录 + 失败重试：handler 多次调用，副作用==1 |
| `RestartRecoveryTest` | 优雅关停后重启恢复 | 未确认消息不丢；已确认消息不重复；位点单调推进 |
| `CrashRecoveryTest` | **真实 SIGKILL** 硬崩溃 | 子进程 kill -9 后重启：未确认/排队消息各副作用1，已确认消息0调用 |
| `RetryAndDeadLetterTest` | 失败分类、退避、死信 | RETRIES_EXHAUSTED vs NON_RETRYABLE vs 异常默认可重试；原始载荷保留 |
| `DeadLetterReplayTest` | 死信重放、重放约束 | 重放成功副作用==1；再次重放被去重；重放终态跨重启保持 |
| `CapacityAndShutdownTest` | 容量边界、优雅关停 | 两类拒绝为不同异常类型；关停排空在途消息；重启不重复 |

所有队列组件日志均包含 **主题 / 分区（topic/partition）、位点（offset）、
消息标识（messageId）、判定（decision）、失败分类（kind）、判定依据（basis）**，
例如：

```
INFO ... handle   topic=retry partition=0 offset=2 messageId=boom attempt=3 replay=false decision=RETRY kind=RETRYABLE basis=thrownException reason=IllegalStateException: kaboom
INFO ... deadletter topic=retry partition=0 offset=2 messageId=boom decision=DEAD_LETTER kind=RETRIES_EXHAUSTED attempts=3 basis=deadletterPersisted
INFO ... dedup-skip topic=dlq partition=0 offset=2 messageId=recoverable replay=true decision=SKIP basis=ledgerShowsTerminal
```

`CrashRecoveryTest` 通过 `ProcessBuilder` 启动独立 JVM（`tests/harness/CrashHarness`），
用 `destroyForcibly()`（SIGKILL）制造硬崩溃，再用同一数据目录启动第二个进程接管，
以真实进程边界验证恢复（而非同 JVM 内遗弃对象）。

---

## 8. 主要 API 速查

```text
BrokerConfig(backoffBase, maxAttempts, partitionPrefetch, shutdownAwait, handleTimeout)
BrokerConfig.fast()

LocalMessageBroker(dataDir, topic, partitionCount, config, MessageHandler)
  start() / shutdown() / close()
  send(payload)                         # 轮询分区 + UUID
  send(payload, messageId)              # 按 id hash 稳定路由
  sendToPartition(p, payload, id)       # 指定分区
  deadLetters() : List<DeadLetterRecord>
  replayDeadLetter(id) / replayAllDeadLetters()
  awaitDrained(timeoutMillis) : boolean
  backlog(partition) : long

MessageHandler.handle(Message) : HandleResult     # ack / retry / deadLetter
Message: getId/getTopic/getPartition/getOffset/getPayload/isReplay
DeadLetterRecord: getOriginalMessage/getFailureKind/getReason/getAttempts/getDeadLetteredAt
```

## 9. 设计边界（非目标）

- 单进程内多线程；不是网络服务、不做跨进程的分区协调（消费者组语义体现为
  「同目录重启即接管」，而非多消费者同时 rebalance）。
- 去重账本 / 死信日志为追加式、不做压缩与 GC（面向本地验证场景）。
- 不保证业务外部副作用的跨系统原子性（见 §3.3 的幂等边界）。
