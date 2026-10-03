package com.github.lisama569.localmessagequeuejava.mq.queue;

import com.github.lisama569.localmessagequeuejava.mq.exception.MessageRejectedException;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;

import java.util.Collection;
import java.util.List;

/**
 * 本地消息队列：主题/分区语义、容量控制与位点读取。
 */
public interface MessageQueue {

    /**
     * 发送消息；达到分区容量时抛 MessageRejectedException(QUEUE_FULL)，关停时为 SHUTTING_DOWN。
     */
    Message send(String topic, int partition, String messageId, String payload) throws MessageRejectedException;

    /**
     * 读取某分区位点上的消息；位点无数据返回 null。
     */
    Message fetch(TopicPartition tp, long offset);

    /**
     * 下一条消息将分配到的位点。
     */
    long nextOffset(TopicPartition tp);

    /**
     * 当前积压条数（物理日志条数）。
     */
    long backlog(TopicPartition tp);

    /**
     * 当前尚未被确认而占用容量的消息条数。
     */
    long unacked(TopicPartition tp);

    /**
     * 消息被确认后释放其占用的容量。
     */
    void releaseOne(TopicPartition tp);

    /**
     * 消费组启动/接管时，按已确认位点与物理存活区间重建未确认计数。
     */
    void reconcileUnacked(TopicPartition tp, long committedOffset);

    /**
     * 分区容量上限。
     */
    long capacity(TopicPartition tp);

    Collection<TopicPartition> partitions();

    List<Message> drainRange(TopicPartition tp, long fromOffset, long toOffset);

    /**
     * 物理丢弃 offset &lt; truncateBefore 的已确认历史日志以释放容量（仅允许用于已确认数据）。
     */
    void truncateBefore(TopicPartition tp, long truncateBefore);

    void shutdown();
}
