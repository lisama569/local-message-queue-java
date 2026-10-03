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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 门面服务：装配队列、存储与消费者，提供发送/订阅/死信/重放/关停能力。
 * 数据目录布局：topics/（消息日志）、groups/&lt;group&gt;/（位点、去重集合、死信）。
 */
public class MqService implements AutoCloseable {

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

    public MessageConsumer registerConsumer(String group, MessageHandler handler, ConsumerOptions options,
                                            List<TopicPartition> partitions) {
        GroupStores shared = storesOf(group);
        MessageConsumer existing = consumers.get(group);
        if (existing != null && existing.isRunning()) {
            throw new IllegalStateException("消费组已存在运行中的订阅者，热替换请使用 replaceConsumer: group=" + group);
        }
        MessageConsumer consumer = new MessageConsumer(group, queue, handler, shared.stateStore(),
                shared.processedIdStore(), shared.deadLetterStore(), options);
        consumer.subscribe(partitions);
        consumer.start();
        consumers.put(group, consumer);
        return consumer;
    }

    /**
     * 订阅者不停机热替换（灰度升级/回滚）：
     * <ol>
     *   <li>优雅关停旧订阅者：停止拉取新任务，等待在途处理完成并落盘，保证切换窗口内
     *       新老订阅者不会同时处理同一分区；</li>
     *   <li>用新处理器启动新订阅者，从共享的已确认位点/未确认队头/暂停标志继续，
     *       已确认消息命中去重不再产生副作用，未确认消息不丢失。</li>
     * </ol>
     */
    public MessageConsumer replaceConsumer(String group, MessageHandler newHandler, ConsumerOptions options,
                                           List<TopicPartition> partitions) {
        MessageConsumer old = consumers.remove(group);
        if (old != null) {
            old.close();
        }
        return registerConsumer(group, newHandler, options, partitions);
    }

    /** 按分区暂停某消费组的投递（幂等）。 */
    public void pause(String group, TopicPartition tp) {
        requireConsumer(group).pause(tp);
    }

    /** 按分区恢复某消费组的投递（幂等），从暂停位置继续。 */
    public void resume(String group, TopicPartition tp) {
        requireConsumer(group).resume(tp);
    }

    public boolean isPaused(String group, TopicPartition tp) {
        MessageConsumer consumer = consumers.get(group);
        return consumer != null && consumer.isPaused(tp);
    }

    /**
     * 按分区查询积压与容量水位：未确认条数、已确认位点、距容量上限剩余条数、暂停状态。
     */
    public PartitionWatermark watermark(String group, TopicPartition tp) {
        return queue.watermark(tp, isPaused(group, tp));
    }

    private MessageConsumer requireConsumer(String group) {
        MessageConsumer consumer = consumers.get(group);
        if (consumer == null || !consumer.isRunning()) {
            throw new IllegalStateException("消费组不存在运行中的订阅者: group=" + group);
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
