package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.consumer.ConsumerOptions;
import com.github.lisama569.localmessagequeuejava.mq.consumer.MessageConsumer;
import com.github.lisama569.localmessagequeuejava.mq.handler.MessageHandler;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
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
        GroupStores stores = storesOf(group);
        MessageConsumer consumer = new MessageConsumer(group, queue, handler, stores.stateStore(),
                stores.processedIdStore(), stores.deadLetterStore(), options);
        consumer.subscribe(partitions);
        consumer.start();
        consumers.put(group, consumer);
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
