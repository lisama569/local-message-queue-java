package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.consumer.ConsumerOptions;
import com.github.lisama569.localmessagequeuejava.mq.consumer.MessageConsumer;
import com.github.lisama569.localmessagequeuejava.mq.handler.MessageHandler;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.model.PartitionWatermark;
import com.github.lisama569.localmessagequeuejava.mq.model.ReplayOutcome;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;
import com.github.lisama569.localmessagequeuejava.mq.queue.LocalMessageQueue;
import com.github.lisama569.localmessagequeuejava.mq.queue.MessageQueue;
import com.github.lisama569.localmessagequeuejava.mq.store.DeadLetterStore;
import com.github.lisama569.localmessagequeuejava.mq.store.FileGroupStateStore;
import com.github.lisama569.localmessagequeuejava.mq.store.ProcessedIdStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 门面服务：装配队列、存储与消费者，提供发送/订阅/热替换/暂停恢复/水位/死信/重放/关停能力。
 * 数据目录布局：topics/（消息日志）、groups/&lt;group&gt;/（位点、去重集合、死信）。
 */
public class MqService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MqService.class);

    private final Path dataDir;
    private final LocalMessageQueue queue;
    private final Map<String, GroupStores> groupStores = new ConcurrentHashMap<>();
    private final Map<String, MessageConsumer> consumers = new ConcurrentHashMap<>();

    private record GroupStores(FileGroupStateStore stateStore, ProcessedIdStore processedIdStore,
                               DeadLetterStore deadLetterStore) {
    }

    public MqService(Path dataDir, long perPartitionCapacity) {
        this.dataDir = dataDir;
        this.queue = new LocalMessageQueue(dataDir, perPartitionCapacity);
    }

    private GroupStores storesOf(String group) {
        return groupStores.computeIfAbsent(group, g -> {
            Path dir = dataDir.resolve("groups").resolve(g);
            return new GroupStores(new FileGroupStateStore(dir.resolve("state.json")),
                    new ProcessedIdStore(dir.resolve("processed.log")),
                    new DeadLetterStore(dir.resolve("dlq.log")));
        });
    }

    public Message send(String topic, int partition, String messageId, String payload) {
        return queue.send(topic, partition, messageId, payload);
    }

    /**
     * 注册并启动消费组。同一 group 已有在运订阅者时显式拒绝，
     * 热替换请使用 {@link #replaceConsumer(String, MessageHandler, ConsumerOptions, List)}。
     */
    public MessageConsumer registerConsumer(String group, MessageHandler handler, ConsumerOptions options,
                                            List<TopicPartition> partitions) {
        MessageConsumer existing = consumers.get(group);
        if (existing != null && existing.isRunning()) {
            throw new IllegalStateException(
                    "消费组已有在运订阅者，热替换请调用 replaceConsumer: group=" + group);
        }
        return startConsumer(group, handler, options, partitions);
    }

    /**
     * 订阅者不停机热替换（灰度升级/回滚）：
     * <ol>
     *   <li>先优雅关停旧订阅者（停止拉取，等待在途处理完成并落盘）；</li>
     *   <li>再以同一组的位点/去重集合/死信存储启动新订阅者接管分区。</li>
     * </ol>
     * 切换窗口内同一分区不可能被新老订阅者同时处理；未确认消息不丢，
     * 已确认消息不再产生副作用，新订阅者从正确的未确认位置继续。
     */
    public synchronized MessageConsumer replaceConsumer(String group, MessageHandler handler,
                                                        ConsumerOptions options,
                                                        List<TopicPartition> partitions) {
        MessageConsumer old = consumers.get(group);
        if (old == null || !old.isRunning()) {
            throw new IllegalStateException("消费组无在运订阅者，无法热替换: group=" + group);
        }
        List<TopicPartition> targetPartitions =
                (partitions == null || partitions.isEmpty()) ? List.copyOf(old.subscriptions()) : partitions;
        log.info("[热替换] group={} 判定=开始关停旧订阅者 分区={}", group, targetPartitions);
        old.close();
        log.info("[热替换] group={} 判定=旧订阅者已完全停止，启动新订阅者接管", group);
        return startConsumer(group, handler, options, targetPartitions);
    }

    private MessageConsumer startConsumer(String group, MessageHandler handler, ConsumerOptions options,
                                          List<TopicPartition> partitions) {
        GroupStores stores = storesOf(group);
        MessageConsumer consumer = new MessageConsumer(group, queue, handler, stores.stateStore(),
                stores.processedIdStore(), stores.deadLetterStore(), options);
        consumer.subscribe(partitions);
        consumer.start();
        consumers.put(group, consumer);
        return consumer;
    }

    /** 暂停某消费组指定分区的投递（在途处理不受影响，未确认状态与顺序保留，可重复调用）。 */
    public boolean pause(String group, TopicPartition tp) {
        return requireConsumer(group).pause(tp);
    }

    /** 恢复某消费组指定分区的投递（从暂停前位置继续，可重复调用）。 */
    public boolean resume(String group, TopicPartition tp) {
        return requireConsumer(group).resume(tp);
    }

    /** 查询指定分区的积压与容量水位：未确认条数、容量上限、剩余可用。 */
    public PartitionWatermark watermark(TopicPartition tp) {
        long unacked = queue.unacked(tp);
        long capacity = queue.capacity(tp);
        return new PartitionWatermark(tp.topic(), tp.partition(), unacked, capacity,
                Math.max(0, capacity - unacked));
    }

    /** 查询所有已出现分区的积压与容量水位。 */
    public Map<TopicPartition, PartitionWatermark> watermarks() {
        Map<TopicPartition, PartitionWatermark> result = new java.util.HashMap<>();
        queue.partitions().forEach(tp -> result.put(tp, watermark(tp)));
        return result;
    }

    private MessageConsumer requireConsumer(String group) {
        MessageConsumer consumer = consumers.get(group);
        if (consumer == null || !consumer.isRunning()) {
            throw new IllegalStateException("消费组无在运订阅者: group=" + group);
        }
        return consumer;
    }

    public List<DeadLetterRecord> deadLetters(String group) {
        return storesOf(group).deadLetterStore().findAll();
    }

    /**
     * 重放死信：以原消息标识重新投递。若该消息已被确认过，
     * 消费端按去重规则跳过副作用（不绕过重复消费约束）。
     */
    public ReplayOutcome replayDeadLetter(String group, String messageId) {
        GroupStores stores = storesOf(group);
        DeadLetterRecord record = stores.deadLetterStore().find(messageId)
                .orElseThrow(() -> new IllegalArgumentException("死信不存在: group=" + group + " id=" + messageId));
        boolean deduplicated = stores.processedIdStore().contains(messageId);
        Message msg = queue.send(record.topic(), record.partition(), record.messageId(), record.payload());
        return new ReplayOutcome(messageId, msg.offset(), deduplicated);
    }

    public Map<TopicPartition, Long> committedOffsets(String group) {
        MessageConsumer consumer = consumers.get(group);
        if (consumer != null) {
            return consumer.committedOffsets();
        }
        return storesOf(group).stateStore().loadAll().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().committedOffset()));
    }

    public MessageQueue queue() {
        return queue;
    }

    /**
     * 优雅关停：先停止接收新消息，再依次关停消费者（在途处理完成并落盘）。
     */
    @Override
    public void close() {
        queue.shutdown();
        consumers.values().forEach(MessageConsumer::close);
    }
}
