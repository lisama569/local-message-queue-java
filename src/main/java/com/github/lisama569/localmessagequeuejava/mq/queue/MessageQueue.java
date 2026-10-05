package com.github.lisama569.localmessagequeuejava.mq.queue;

import com.github.lisama569.localmessagequeuejava.mq.exception.MessageRejectedException;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;

import java.util.Collection;
import java.util.List;

/**
 * 本地消息队列：主题/分区语义、容量控制与位点读取。
 * 容量与积压均以“尚未被确认的消息条数”计，确认位点持久化，重启后不依赖消费者即可恢复。
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
     * 物理日志的起始位点（被裁剪后大于 0）。
     */
    long baseOffset(TopicPartition tp);

    /**
     * 当前积压条数：尚未被确认、仍占用容量的消息条数（与是否裁剪物理日志无关）。
     */
    long backlog(TopicPartition tp);

    /**
     * 当前尚未被确认而占用容量的消息条数（与 {@link #backlog(TopicPartition)} 同口径）。
     */
    long unacked(TopicPartition tp);

    /**
     * 将分区的确认位点推进到 committedOffset（单调不减）：
     * 重建未确认计数、释放已确认消息占用的容量，并把确认水位持久化；
     * 消费组启动/接管时与每条消息确认时都调用，重启后无需消费者即可恢复容量口径。
     */
    void commit(TopicPartition tp, long committedOffset);

    /**
     * 分区容量上限。
     */
    long capacity(TopicPartition tp);

    Collection<TopicPartition> partitions();

    List<Message> drainRange(TopicPartition tp, long fromOffset, long toOffset);

    /**
     * 物理丢弃 offset &lt; truncateBefore 的已确认历史日志以释放磁盘（仅允许用于已确认数据；
     * 容量不依赖裁剪，裁剪与否不影响积压与剩余容量口径）。
     */
    void truncateBefore(TopicPartition tp, long truncateBefore);

    void shutdown();
}
