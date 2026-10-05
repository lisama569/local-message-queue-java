# local-message-queue-java

纯本地、零外部依赖的消息消费、确认与重试编排实现（Spring Boot 3 / Java 21）。
不依赖任何外部中间件、真实凭据或网络资源，所有状态落在本地文件系统，可本地运行与验证。

## 能力总览

- 主题 / 分区 / 消费者组语义：同一分区内按位点严格有序处理，不同分区由独立 worker 并发推进
- **订阅者不停机热替换**：同一消费组可在运行中换成新处理逻辑（灰度升级 / 回滚），新老订阅者不同时处理同一分区
- **按分区暂停 / 恢复投递**：只停指定分区，未确认状态与顺序保留，恢复后从原位点继续（可反复调用）
- 失败分类、指数退避重试与死信（DLQ），三类失败结果可区分
- 重启恢复：位点、未确认队头、已确认去重集合、暂停标记、死信全部持久化，进程中断后状态一致
- **积压与容量水位**：容量按“尚未确认的消息”实时计量，确认即释放；确认水位持久化，
  重启后消费者尚未接管时积压与剩余容量即已准确；可按分区查询积压 / 容量 / 剩余
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
# 查询所有分区的积压/容量水位（backlog=未确认条数，remaining=离上限还可接收多少）
curl 'localhost:8080/api/mq/watermarks'
# 按分区查询积压（与水位同口径：未确认条数，重启后消费者未接管时也准确）
curl 'localhost:8080/api/mq/backlog?topic=orders&partitions=2'
# 暂停 / 恢复某消费组在指定分区上的投递（changed 表示状态是否发生变化，重复调用为 false）
curl -X POST 'localhost:8080/api/mq/pause?group=g1&topic=orders&partition=0'
curl -X POST 'localhost:8080/api/mq/resume?group=g1&topic=orders&partition=0'
# 查询 / 重放死信
curl 'localhost:8080/api/mq/dead-letters?group=g1'
curl -X POST 'localhost:8080/api/mq/dead-letters/replay?group=g1&messageId=m1'
```

编程式使用见 `MqService` 与测试用例（`src/test/java/.../mq/`）：

```java
// 运行中把 g1 的处理逻辑换成 newHandler（灰度升级；出问题可再换回去回滚）
service.replaceConsumer("g1", newHandler, ConsumerOptions.defaults(), null); // null=沿用原订阅分区
// 只暂停 / 恢复某个分区
service.pause("g1", new TopicPartition("orders", 0));
service.resume("g1", new TopicPartition("orders", 0));
// 查询单分区 / 全部分区水位
PartitionWatermark wm = service.watermark(new TopicPartition("orders", 0)); // backlog/capacity/remaining
Map<TopicPartition, PartitionWatermark> all = service.watermarks();
```

## 交付语义

- **至少一次投递 + 框架级去重**：消息确认（ack）前先写已确认集合（`processed.log`，fsync），
  再推进位点（`state.json`，临时文件 + 原子 move + fsync）。重复投递、死信重放、
  崩溃恢复导致的再次投递，都会在消费侧命中去重而**不产生重复副作用**。
- **分区内有序**：每个分区同一时刻至多一条队头消息处于处理中（IN_FLIGHT），
  前一条未确认（成功/死信）不会投递下一条；退避中的消息会阻塞该分区后续消息（保证顺序）。
- **跨分区并发**：每个分区独立的单线程 worker，互不阻塞。
- **业务幂等边界**：若处理器在“副作用已发生但确认未落盘”的窗口内被kill，
  框架无法感知该副作用——业务副作用应与消息 id 幂等绑定（框架去重键即消息 id）。

## 订阅者热替换（灰度升级 / 回滚）

`MqService.replaceConsumer(group, handler, options, partitions)` 在**不停服务、不丢订阅**的前提下
替换同一消费组的处理逻辑（`partitions` 传 `null` 表示沿用旧订阅的分区集合）。

切换严格按“**先完全停止旧订阅者，再启动新订阅者**”的顺序进行：

1. 旧订阅者停止拉取新任务，协调线程退出；
2. 等待旧订阅者的在途处理完成并落盘（宽限期 = `max(5s, processingTimeout + 2s)`），超时则中断在途处理；
3. 新订阅者复用同一组的 `state.json` / `processed.log` / `dlq.log` 启动，按已确认位点接管各分区。

**切换窗口内的保证：**

- 同一分区在任一时刻只属于一个订阅者，**不可能出现新老订阅者同时处理同一分区**；
- **未确认消息不丢**：切换时仍未确认的队头（含被中断的在途处理）按中断恢复规则落盘为退避重投，
  由新订阅者从该未确认位置继续；
- **已确认消息不重**：已确认位点之前的消息不再投递；即使消息被新逻辑再次见到，
  `processed.log` 去重集合也保证不产生第二次副作用；
- 分区内顺序与退避/死信计数在切换前后连续。

注意：在途处理被超时中断属于“副作用未知”窗口，业务副作用仍须与消息 id 幂等绑定（见业务幂等边界）。
同一 group 直接再次调用 `registerConsumer` 会抛 `IllegalStateException`，以防误用造成两个订阅者并存；
热替换必须走 `replaceConsumer`。

## 按分区暂停 / 恢复投递

`MqService.pause(group, tp)` / `resume(group, tp)`（HTTP：`POST /api/mq/pause|resume`）
只作用于指定消费组的指定分区：

- **暂停期间**：不再拉取新消息，退避到期的消息也不会重投；
  但暂停瞬间**已经在途**的处理允许跑完（其确认/死信正常落盘）。
- **状态保留**：已确认位点、未确认队头（含退避状态与尝试次数）、分区内顺序全部原样保留，
  暂停期间新发送的消息可入队占容量但不会被投递。
- **恢复后**：从暂停前的位点/队头继续，**不跳过任何消息**，顺序不变；
  其他分区全程不受影响、照常并发推进。
- **可反复调用**：重复暂停 / 重复恢复是幂等的（返回 `changed=false`），不会出现暂停失效或恢复后跳消息。
- 暂停标记持久化在 `state.json` 中，**进程重启后仍保持暂停**，需显式 `resume` 才恢复投递。

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
  <dataDir>/committed-offsets.json                # 每分区确认水位（单调不减，原子写）
  <dataDir>/groups/<group>/state.json             # 确认位点 + 未确认队头 + 暂停标记（原子写）
  <dataDir>/groups/<group>/processed.log          # 已确认消息 id 集合
  <dataDir>/groups/<group>/dlq.log                # 死信记录
  ```

## 积压、容量水位与优雅关停

- 每分区容量上限（`perPartitionCapacity`）以**尚未被确认的消息条数**计量：
  每发送一条占 1 个名额，消息一旦被确认（处理成功 / 去重跳过 / 进入死信）其名额**立即释放**，
  随后即可接收新消息——**不依赖**日志裁剪，不会出现“积压已清空却仍一直拒收”。
- **确认后容量释放与重启恢复**：每条消息确认时，队列把该分区的确认水位
  （已确认到的位点，单调不减）持久化到 `<dataDir>/committed-offsets.json`（原子写 + fsync）。
  重启后无论消费者是否已接管、物理日志是否被裁剪，分区未确认条数都按
  `末端位点 − max(确认水位, 物理起始位点)` 立即恢复：**已确认的消息不再占容量**，
  积压与剩余容量反映真实未确认条数，新发送可正常被接收；各分区水位各自独立、互不影响。
  从未有过确认记录的分区按物理存活条数保守计量（不会超发容量）。
- 水位查询（`MqService.watermark(tp)` / `watermarks()`，HTTP：`GET /api/mq/watermarks`）按分区返回：
  - `backlog`：当前未确认、仍占用容量的消息条数（消费者未接管时同口径）；
  - `capacity`：该分区容量上限；
  - `remaining = capacity - backlog`：离上限还能接收多少（用于判断该扩容还是消费卡住）。
  `queue.backlog(tp)`（HTTP：`GET /api/mq/backlog`）与水位同为未确认口径；
  物理保留情况可另看 `baseOffset` / `nextOffset`。
- 消费组启动 / 热替换接管分区时，同样按确认水位推进未确认计数，与每条确认走同一口径。
- 达到上限时 `send` 抛出 `MessageRejectedException(QUEUE_FULL)`；
  关停期间抛出 `MessageRejectedException(SHUTTING_DOWN)`，两种拒绝原因明确可区分（分类与旧版本一致）。
- 容量口径与消费者组一一对应，沿用“单消费者组消费一个分区”的既有兼容范围；
  `truncateCommittedLog(true)` 仍可选开启，用于物理裁剪已确认历史日志、控制磁盘占用（位点单调不复用），
  裁剪与否不影响积压与剩余容量口径。
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
| `CapacityBoundaryTest` | `QUEUE_FULL` / `SHUTTING_DOWN` 拒绝可区分；裁剪释放容量（旧口径回归） |
| `HotSwapConsumerTest` | 热替换不重不漏、从首个未确认位置继续；切换窗口无双跑；重复注册被拒 |
| `PauseResumeTest` | 暂停期间停投但在途可完成、恢复后按序不跳消息；分区隔离；反复暂停/恢复稳定 |
| `PauseRestartTest` | 暂停标记落盘，重启后仍暂停，恢复后从未确认位置继续 |
| `BacklogWatermarkTest` | 确认即释放容量（不依赖裁剪）、积压清空立即复用、按分区水位、拒绝原因不回归 |
| `RestartCapacityReuseTest` | 确认完→重启→消费者未接管：容量立即复用、按分区隔离、裁剪与否口径一致、拒绝原因分类不回归 |

测试日志均打印分区、位点、消息标识与判定依据
（`[投递]/[确认]/[退避]/[死信]/[去重]/[恢复]/[暂停]/[恢复投递]/[热替换]/[容量]/[断言]`）。

本地手动验证“确认完 → 重启 → 消费者未接管”路径：

```bash
# 1. 启动服务（容量默认见 mq.per-partition-capacity），发送若干消息并等消费组确认
curl -X POST localhost:8080/api/mq/send -H 'Content-Type: application/json' \
  -d '{"topic":"orders","partition":0,"messageId":"m1","payload":"hello"}'
curl 'localhost:8080/api/mq/watermarks'   # backlog 归 0 即确认完
# 2. 停掉进程，用同一 --mq.data-dir 重新启动（先不注册/等待消费者）
# 3. 消费者尚未接管时直接查水位：已确认消息不再计入积压，remaining 已恢复
curl 'localhost:8080/api/mq/watermarks'
curl 'localhost:8080/api/mq/backlog?topic=orders&partitions=1'
# 4. 继续发送新消息，可正常被接收（不再被误拒 QUEUE_FULL）
```

## 兼容范围

- 新能力均为**增量扩展**，未改变既有发送 / 订阅 / 重试 / 死信 / 关停语义与异常分类。
- 旧的 `state.json`（无 `paused` 字段）可被新版本直接读取，缺省按“未暂停”处理。
- 容量计量从“物理日志条数”改为“未确认条数”：在单消费者组场景下两者结论一致，
  且确认后无需裁剪即可复用容量；确认水位持久化在 `committed-offsets.json`，
  重启后消费者尚未接管时 `backlog` / `watermark` 即按真实未确认条数返回，
  不再把已确认消息计入积压或容量占用。
- 订阅者热替换与暂停/恢复均按“消费组 × 分区”生效，假设一个分区同一时间只被一个组的一个订阅者接管
  （与现有单活订阅模型一致）。
