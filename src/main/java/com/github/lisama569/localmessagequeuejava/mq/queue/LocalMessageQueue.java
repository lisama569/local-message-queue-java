package com.github.lisama569.localmessagequeuejava.mq.queue;

import com.github.lisama569.localmessagequeuejava.mq.exception.MessageRejectedException;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.model.RejectReason;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;
import com.github.lisama569.localmessagequeuejava.mq.store.FileMessageLogStore;
import com.github.lisama569.localmessagequeuejava.mq.store.MessageLogStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地消息队列实现：委派文件日志，维护关停标志与每分区容量边界。
 * 容量以“尚未被确认的消息条数”计：发送占 1，确认释放 1，确认后容量立即可复用。
 */
public class LocalMessageQueue implements MessageQueue {

    private static final Logger log = LoggerFactory.getLogger(LocalMessageQueue.class);

    private final MessageLogStore logStore;
    private final long perPartitionCapacity;
    private final Set<TopicPartition> knownPartitions = ConcurrentHashMap.newKeySet();
    private final Map<TopicPartition, AtomicLong> unackedCounts = new ConcurrentHashMap<>();
    private volatile boolean shuttingDown;

    public LocalMessageQueue(Path dataDir, long perPartitionCapacity) {
        this(new FileMessageLogStore(dataDir.resolve("topics")), perPartitionCapacity);
    }

    public LocalMessageQueue(MessageLogStore logStore, long perPartitionCapacity) {
        this.logStore = logStore;
        this.perPartitionCapacity = perPartitionCapacity;
    }

    private AtomicLong counter(TopicPartition tp) {
        // 无消费组确认信息时，物理日志条数即未确认条数（保守口径，不会超发容量）
        return unackedCounts.computeIfAbsent(tp, k -> new AtomicLong(logStore.size(k)));
    }

    @Override
    public Message send(String topic, int partition, String messageId, String payload) {
        TopicPartition tp = new TopicPartition(topic, partition);
        if (shuttingDown) {
            throw new MessageRejectedException(RejectReason.SHUTTING_DOWN,
                    "队列正在关停，拒绝接收新消息: " + tp.key() + " id=" + messageId);
        }
        AtomicLong unacked = counter(tp);
        synchronized (unacked) {
            long current = unacked.get();
            if (current >= perPartitionCapacity) {
                throw new MessageRejectedException(RejectReason.QUEUE_FULL,
                        "分区积压达到容量上限 " + perPartitionCapacity + "（未确认 " + current + " 条）: "
                                + tp.key() + " id=" + messageId);
            }
            long offset = logStore.append(tp, messageId, payload);
            unacked.set(current + 1);
            knownPartitions.add(tp);
            return new Message(messageId, topic, partition, offset, payload, System.currentTimeMillis());
        }
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
        return logStore.size(tp);
    }

    @Override
    public long unacked(TopicPartition tp) {
        return counter(tp).get();
    }

    @Override
    public void releaseOne(TopicPartition tp) {
        long v = counter(tp).updateAndGet(cur -> Math.max(0, cur - 1));
        if (v == 0) {
            log.info("[容量] partition={} unacked=0 判定=积压已清空，容量全部可复用", tp.key());
        }
    }

    @Override
    public void reconcileUnacked(TopicPartition tp, long committedOffset) {
        AtomicLong unacked = counter(tp);
        // 与 send 同一把锁，避免消费组启动重建计数时与并发发送互相覆盖
        synchronized (unacked) {
            // 消费组启动/接管时按已确认位点与物理存活区间重建未确认计数：
            // 未确认条数 = 末端位点 - max(已确认位点, 物理起始位点)
            long liveStart = Math.max(committedOffset, logStore.baseOffset(tp));
            long value = Math.max(0, logStore.nextOffset(tp) - liveStart);
            unacked.set(value);
            log.info("[容量] partition={} unacked={} committed={} base={} 判定=按位点重建未确认计数",
                    tp.key(), value, committedOffset, logStore.baseOffset(tp));
        }
    }

    @Override
    public long capacity(TopicPartition tp) {
        return perPartitionCapacity;
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
