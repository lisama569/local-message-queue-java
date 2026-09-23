# local-message-queue-java

纯本地、零外部依赖的消息消费、确认与重试编排实现（Spring Boot 3 / Java 21）。
不依赖任何外部中间件、真实凭据或网络资源，所有状态落在本地文件系统，可本地运行与验证。

## 能力总览

- 主题 / 分区 / 消费者组语义：同一分区内按位点严格有序处理，不同分区由独立 worker 并发推进
- 失败分类、指数退避重试与死信（DLQ），三类失败结果可区分
- 重启恢复：位点、未确认队头、已确认去重集合、死信全部持久化，进程中断后状态一致
- 积压控制与优雅关停：容量边界与关停拒绝原因明确可区分
- 死信重放：保留原始载荷重新投递，且不绕过重复消费约束

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
# 查询 / 重放死信
curl 'localhost:8080/api/mq/dead-letters?group=g1'
curl -X POST 'localhost:8080/api/mq/dead-letters/replay?group=g1&messageId=m1'
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
  <dataDir>/groups/<group>/state.json             # 确认位点 + 未确认队头（原子写）
  <dataDir>/groups/<group>/processed.log          # 已确认消息 id 集合
  <dataDir>/groups/<group>/dlq.log                # 死信记录
  ```

## 积压控制与优雅关停

- 每分区容量上限（`perPartitionCapacity`）：达到上限时 `send` 抛出
  `MessageRejectedException(QUEUE_FULL)`；关停期间抛出 `MessageRejectedException(SHUTTING_DOWN)`，
  两种拒绝原因明确可区分。
- 容量按物理保留日志计算；消费者开启 `truncateCommittedLog(true)` 后，
  每次确认会物理裁剪已提交日志以释放容量（位点单调不复用，仅适用于单消费者组场景）。
- 优雅关停（`MqService.close()`）：先拒收新消息，再停止拉取新任务，
  等待在途处理完成（宽限期 = `max(5s, processingTimeout + 2s)`），超时强制中断；
  所有已开始处理的确认/重试/死信状态均已落盘，下次启动可恢复。

## 验证方法

| 测试 | 覆盖点 |
| --- | --- |
| `ConcurrentConsumptionTest` | 分区内位点严格有序；屏障验证跨分区并发推进 |
| `DuplicateDeliveryTest` | 同一消息 id 重复投递，副作用恰好一次 |
| `RestartRecoveryTest` | 处理超时分类为可重试；重启后未确认消息不丢失、已确认不重复 |
| `DeadLetterReplayTest` | 三类失败可区分；死信保留载荷与原因；重放受去重约束 |
| `CapacityBoundaryTest` | `QUEUE_FULL` / `SHUTTING_DOWN` 拒绝可区分；裁剪释放容量 |

测试日志均打印分区、位点、消息标识与判定依据（`[投递]/[确认]/[退避]/[死信]/[去重]/[恢复]/[断言]`）。
