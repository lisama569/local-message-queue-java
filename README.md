# local-message-queue-java

纯本地、零外部依赖的消息消费、确认与重试编排实现（Spring Boot 3 / Java 21）。
不依赖任何外部中间件、真实凭据或网络资源，所有状态落在本地文件系统，可本地运行与验证。

## 能力总览

- 主题 / 分区 / 消费者组语义：同一分区内按位点严格有序处理，不同分区由独立 worker 并发推进
- 失败分类、指数退避重试与死信（DLQ），三类失败结果可区分
- 重启恢复：位点、未确认队头、已确认去重集合、死信全部持久化，进程中断后状态一致
- 积压控制与优雅关停：容量边界与关停拒绝原因明确可区分
- 死信重放：保留原始载荷重新投递，且不绕过重复消费约束
- **订阅者热替换**：同一消费组运行中无缝更换处理逻辑（灰度/回滚），切换窗口新老不重不漏
- **按分区暂停/恢复投递**：数据修复/临时限流时只停指定分区，位点、顺序、未确认状态全部保留
- **积压/容量水位查询**：积压实时按"未确认消息"计算，确认即释放容量，可按分区查询积压与剩余容量

## 快速开始

```bash
# 运行全部测试（并发消费/重复投递/重启恢复/死信重放/容量边界）
mvn test

# 启动本地服务（启用内置门面与 HTTP API）
mvn spring-boot:run -Dspring-boot.run.arguments="--mq.enabled=true --mq.data-dir=./mq-data"
```

HTTP 验证接口（`mq.enabled=true` 时可用）：

```bash
# 发送消息
curl -X POST localhost:8080/api/mq/send -H 'Content-Type: application/json' \
  -d '{"topic":"orders","partition":0,"messageId":"m1","payload":"hello"}'
# 查询消费组确认位点
curl 'localhost:8080/api/mq/offsets?group=g1'
# 按分区查询积压/容量水位（未确认条数、已确认位点、剩余容量、暂停状态）
curl 'localhost:8080/api/mq/backlog?group=g1&topic=orders&partitions=2'
# 暂停 / 恢复某消费组在指定分区上的投递（重复调用幂等）
curl -X POST 'localhost:8080/api/mq/pause?group=g1&topic=orders&partition=0'
curl -X POST 'localhost:8080/api/mq/resume?group=g1&topic=orders&partition=0'
# 查询 / 重放死信
curl 'localhost:8080/api/mq/dead-letters?group=g1'
curl -X POST 'localhost:8080/api/mq/dead-letters/replay?group=g1&messageId=m1'
```

编程式热替换（灰度升级 / 回滚）见 `MqService.replaceConsumer(...)`：

```java
// 旧订阅者正在运行；切换为新处理器（可再次调用以回滚到旧逻辑）
MessageConsumer v2 = mqService.replaceConsumer("g1", newHandler, options, partitions);
// 按分区暂停 / 恢复（也可在 HTTP 上操作）
mqService.pause("g1", new TopicPartition("orders", 0));
mqService.resume("g1", new TopicPartition("orders", 0));
// 查询某分区积压与容量水位
PartitionWatermark w = mqService.watermark("g1", new TopicPartition("orders", 0));
// w.backlog() 未确认条数；w.remaining() 距容量上限剩余；w.committedOffset() 已确认位点；w.paused()
```

编程式使用见 `MqService` 与测试用例（`src/test/java/.../mq/`）。

## 交付语义

- **至少一次投递 + 框架级去重**：消息确认（ack）前先写已确认集合（`processed.log`，fsync），
  再推进位点（`state.json`，临时文件 + 原子 move + fsync）。重复投递、死信重放、
  崩溃恢复导致的再次投递，都会在消费侧命中去重而**不产生重复副作用**。
- **分区内有序**：每个分区同一时刻至多一条队头消息处于处理中（IN_FLIGHT），
  前一条未确认（成功/死信）不会投递下一条；退避中的消息会阻塞该分区后续消息（保证顺序）。
- **跨分区并发**：每个分区独立的单线程 worker，互不阻塞。
- **业务幂等边界**：若处理器在“副作用已发生但确认未落盘”的窗口内被kill，
  框架无法感知该副作用——业务副作用应与消息 id 幂等绑定（框架去重键即消息 id）。

## 重试与死信规则

| 失败类型 | 触发条件 | 行为 |
| --- | --- | --- |
| `RETRYABLE` | 抛出 `RetryableException`、其它未分类异常、处理超时 | 指数退避后重投（`initialBackoff * multiplier^n`，封顶 `maxBackoff`） |
| `NON_RETRYABLE` | 抛出 `NonRetryableException` | 立即进入死信，不退避 |
| `RETRY_EXHAUSTED` | 可重试失败达到 `maxAttempts` | 进入死信 |

- 死信记录保留**原始载荷、主题/分区/位点、失败类型、失败原因、尝试次数**（`dlq.log`，追加写 + fsync）。
- 死信重放以**原消息 id** 重新入队：若该 id 已被确认过，消费侧按去重规则跳过副作用
  （`ReplayOutcome.deduplicated=true`），重放不绕过重复消费约束；若重放后仍失败，按同样规则再次分类。
- 处理超时（`processingTimeout`）按可重试失败处理，超时会中断处理线程。

## 恢复边界

- 每次状态迁移（投递 / 确认 / 退避 / 死信）**先落盘再推进**，崩溃后只会读到旧状态或新状态，无半写状态。
- 重启时处于 `IN_FLIGHT` 的队头（进程中断点）按可重试失败恢复为退避重投，已确认位点之前的消息不会重复投递。
- 数据目录布局：
  ```
  <dataDir>/topics/<topic>-<partition>/data.log   # 消息日志（追加写 + fsync）
  <dataDir>/topics/<topic>-<partition>/base       # 裁剪后的起始位点
  <dataDir>/topics/<topic>-<partition>/committed  # 已确认位点水位（原子写，容量按此释放）
  <dataDir>/groups/<group>/state.json             # 确认位点 + 未确认队头 + 分区暂停标志（原子写）
  <dataDir>/groups/<group>/processed.log          # 已确认消息 id 集合
  <dataDir>/groups/<group>/dlq.log                # 死信记录
  ```
- 兼容范围：旧的 `state.json` 没有 `paused` 字段时按"未暂停"反序列化；
  分区没有 `committed` 文件时，已确认水位回退为日志起始位点（对存量数据等价于旧行为，
  首个消息确认后即建立水位）。

## 积压与容量水位

- 每分区容量上限（`perPartitionCapacity`）按**未确认消息**计数：
  `backlog = nextOffset - 已确认位点水位`，达到上限时 `send` 抛出
  `MessageRejectedException(QUEUE_FULL)`；关停期间抛出 `MessageRejectedException(SHUTTING_DOWN)`，
  两种拒绝原因明确可区分（拒绝原因分类未变）。
- **消息一确认，占用的容量立即释放**：确认时先在队列层推进并持久化"已确认位点水位"
  （分区目录 `committed` 文件，临时文件 + 原子 move + fsync，位点单调），
  再落盘消费组状态。**不再依赖物理裁剪日志**——即使关闭 `truncateCommittedLog`，
  积压清空后队列也会立刻继续接收新消息，不会出现"积压已空却仍拒收"。
- 水位查询：`MqService.watermark(group, tp)` /
  `GET /api/mq/backlog?group=..&topic=..&partitions=..`，按分区返回：
  `baseOffset`（起始位点）、`nextOffset`（下条位点）、`committedOffset`（已确认水位）、
  `backlog`（未确认条数）、`capacity`（容量上限）、`remaining`（距上限剩余条数）、
  `paused`（该组视角下是否暂停）。
- 分区间容量独立计数，暂停的分区其未确认消息仍占用容量（它们尚未确认）。
- 兼容说明：水位是"单分区单活跃消费组"模型；`truncateCommittedLog(true)` 的物理裁剪仍然支持，
  裁剪会同步抬高水位下限（位点单调不复用）。

## 订阅者热替换（灰度升级 / 回滚）

`MqService.replaceConsumer(group, newHandler, options, partitions)` 在运行中更换同一消费组的处理逻辑：

1. **先优雅关停旧订阅者**：协调线程立即停止拉取新任务，分区 worker 等待在途消息处理完成
   （宽限期 = `max(5s, processingTimeout + 2s)`），所有确认/退避/死信状态落盘后才返回；
2. **再启动新订阅者**：新旧订阅者共享同一套存储（位点状态、已确认集合、死信），
   新订阅者从磁盘恢复后接管这些分区。

切换窗口内的保证：

- **不重**：新订阅者启动前旧订阅者已停止处理，新老不会同时处理同一分区；
  即使在途处理被宽限期强制中断，重启恢复路径 + `processed.log` 去重保证副作用不重复。
- **不漏**：未确认的在途消息按既有崩溃恢复语义（IN_FLIGHT → 退避重投）由新订阅者重新处理；
  已确认位点之前的消息不会重新投递。
- **从正确位置继续**：新订阅者以已确认位点水位为拉取起点，暂停标志同样随状态恢复。
- 同组重复直接注册会被 `IllegalStateException` 拒绝（防止新老并存），热替换必须走 `replaceConsumer`；
  回滚即再次调用、传入旧处理器。

## 按分区暂停 / 恢复投递

- `MqService.pause/resume(group, tp)`（或 `POST /api/mq/pause|resume`），状态写入消费组
  `state.json`，**热替换与重启后都保持**。
- 暂停期间：**不再投递新消息，退避到期也不重投**；暂停瞬间已在途的消息继续处理完成；
  未确认队头（含 RETRY_WAIT 的退避计时与剩余尝试次数）、确认位点、分区内顺序原样保留。
- 恢复后从暂停前的未确认位置继续，退避继续计时，不跳过任何消息；其他分区完全不受影响、照常并发。
- 暂停/恢复均**幂等**：重复暂停仍是暂停、重复恢复仍是恢复，不会出现暂停失效或恢复后跳消息。

## 优雅关停

- 优雅关停（`MqService.close()`）：先拒收新消息（`SHUTTING_DOWN`），再停止拉取新任务，
  等待在途处理完成（宽限期 = `max(5s, processingTimeout + 2s)`），超时强制中断；
  所有已开始处理的确认/重试/死信状态均已落盘，下次启动可恢复。
- 热替换时对旧订阅者执行的是同一套优雅关停流程，保证切换窗口内无新老重叠处理。
- 注意：若业务处理器完全不响应线程中断且单次执行超过宽限期，旧订阅者可能被强制中断；
  该消息会走崩溃恢复路径由新订阅者重试，业务副作用仍需与消息 id 幂等绑定。

## 验证方法

| 测试 | 覆盖点 |
| --- | --- |
| `ConcurrentConsumptionTest` | 分区内位点严格有序；屏障验证跨分区并发推进 |
| `DuplicateDeliveryTest` | 同一消息 id 重复投递，副作用恰好一次 |
| `RestartRecoveryTest` | 处理超时分类为可重试；重启后未确认消息不丢失、已确认不重复 |
| `DeadLetterReplayTest` | 三类失败可区分；死信保留载荷与原因；重放受去重约束 |
| `CapacityBoundaryTest` | `QUEUE_FULL` / `SHUTTING_DOWN` 拒绝可区分；裁剪释放容量 |
| `HotSwapTest` | 热替换切换窗口新老不同时处理同一分区；在途中断不丢、确认不重、从正确位点继续；暂停跨替换保留；重复注册被拒 |
| `PauseResumeTest` | 暂停时在途跑完、新消息/退避重投挂起；恢复后顺序不跳；其他分区照常并发；反复暂停/恢复幂等稳定 |
| `BacklogCapacityTest` | 不裁剪日志时确认即释放容量、容量可立刻复用；水位字段查询；水位与暂停跨重启保持 |

测试日志均打印分区、位点、消息标识与判定依据（`[投递]/[确认]/[退避]/[死信]/[去重]/[恢复]/[断言]`）。
