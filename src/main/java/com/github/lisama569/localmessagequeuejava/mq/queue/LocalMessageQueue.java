package com.github.lisama569.localmessagequeuejava.mq.queue;

import com.github.lisama569.localmessagequeuejava.mq.exception.MessageRejectedException;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.model.PartitionWatermark;
import com.github.lisama569.localmessagequeuejava.mq.model.RejectReason;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;
import com.github.lisama569.localmessagequeuejava.mq.store.FileMessageLogStore;
import com.github.lisama569.localmessagequeuejava.mq.store.MessageLogStore;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地消息队列实现：委派文件日志，维护关停标志与每分区容量边界。
 */
public class LocalMessageQueue implements MessageQueue {

    private final MessageLogStore logStore;
    private final long perPartitionCapacity;
    private final Set<TopicPartition> knownPartitions = ConcurrentHashMap.newKeySet();
    private volatile boolean shuttingDown;

    public LocalMessageQueue(Path dataDir, long perPartitionCapacity) {
        this(new FileMessageLogStore(dataDir.resolve("topics")), perPartitionCapacity);
    }

    public LocalMessageQueue(MessageLogStore logStore, long perPartitionCapacity) {
        this.logStore = logStore;
        this.perPartitionCapacity = perPartitionCapacity;
    }

    @Override
    public Message send(String topic, int partition, String messageId, String payload) {
        TopicPartition tp = new TopicPartition(topic, partition);
        if (shuttingDown) {
            throw new MessageRejectedException(RejectReason.SHUTTING_DOWN,
                    "队列正在关停，拒绝接收新消息: " + tp.key() + " id=" + messageId);
        }
        if (backlog(tp) >= perPartitionCapacity) {
            throw new MessageRejectedException(RejectReason.QUEUE_FULL,
                    "分区未确认积压达到容量上限 " + perPartitionCapacity + ": " + tp.key() + " id=" + messageId);
        }
        long offset = logStore.append(tp, messageId, payload);
        knownPartitions.add(tp);
        return new Message(messageId, topic, partition, offset, payload, System.currentTimeMillis());
    }

    @Override
    public Message fetch(TopicPartition tp, long offset) {
        List<MessageLogStore.LogEntry> entries = logStore.read(tp, offset, offset + 1);
        if (entries.isEmpty()) {
            return null;
        }
        MessageLogStore.LogEntry e = entries.get(0);
        return new Message(e.messageId(), tp.topic(), tp.partition(), e.offset(), e.payload(), e.createdAtMs());
    }

    @Override
    public long nextOffset(TopicPartition tp) {
        return logStore.nextOffset(tp);
    }

    @Override
    public long backlog(TopicPartition tp) {
        return Math.max(0, logStore.nextOffset(tp) - logStore.loadCommittedOffset(tp));
    }

    @Override
    public long capacity(TopicPartition tp) {
        return perPartitionCapacity;
    }

    @Override
    public PartitionWatermark watermark(TopicPartition tp, boolean paused) {
        long base = logStore.baseOffset(tp);
        long next = logStore.nextOffset(tp);
        long committed = logStore.loadCommittedOffset(tp);
        long unacked = Math.max(0, next - committed);
        long remaining = Math.max(0, perPartitionCapacity - unacked);
        return new PartitionWatermark(tp.topic(), tp.partition(), base, next, committed,
                unacked, perPartitionCapacity, remaining, paused);
    }

    @Override
    public void markCommitted(TopicPartition tp, long committedOffset) {
        logStore.saveCommittedOffset(tp, committedOffset);
    }

    @Override
    public Collection<TopicPartition> partitions() {
        return Set.copyOf(knownPartitions);
    }

    @Override
    public List<Message> drainRange(TopicPartition tp, long fromOffset, long toOffset) {
        return logStore.read(tp, fromOffset, toOffset).stream()
                .map(e -> new Message(e.messageId(), tp.topic(), tp.partition(), e.offset(), e.payload(),
                        e.createdAtMs()))
                .toList();
    }

    /**
     * 物理丢弃已确认的历史日志（释放容量）。
     */
    public void truncateBefore(TopicPartition tp, long truncateBefore) {
        logStore.truncateBefore(tp, truncateBefore);
    }

    @Override
    public void shutdown() {
        shuttingDown = true;
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }
}
