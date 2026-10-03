package com.github.lisama569.localmessagequeuejava.mq.queue;

import com.github.lisama569.localmessagequeuejava.mq.exception.MessageRejectedException;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.model.PartitionWatermark;
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
     * 当前积压条数：尚未被确认的消息数 = nextOffset - 已确认位点水位。
     * 消息一旦确认，占用的容量立即释放。
     */
    long backlog(TopicPartition tp);

    /**
     * 分区容量上限。
     */
    long capacity(TopicPartition tp);

    /**
     * 分区积压/容量水位（含已确认位点、剩余可接收条数）。
     *
     * @param paused 调用方（消费组视角）的暂停状态，仅用于展示
     */
    PartitionWatermark watermark(TopicPartition tp, boolean paused);

    /**
     * 推进已确认位点水位（该位点之前的消息均已确认），单调递增，确认后立即释放容量。
     */
    void markCommitted(TopicPartition tp, long committedOffset);

    Collection<TopicPartition> partitions();

    List<Message> drainRange(TopicPartition tp, long fromOffset, long toOffset);

    /**
     * 物理丢弃 offset &lt; truncateBefore 的已确认历史日志以释放容量（仅允许用于已确认数据）。
     */
    void truncateBefore(TopicPartition tp, long truncateBefore);

    void shutdown();
}
