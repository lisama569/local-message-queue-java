package com.github.lisama569.localmessagequeuejava.mq.store;

import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;

import java.util.List;

/**
 * 分区消息日志：追加写、位点读取、重启后可恢复。
 */
public interface MessageLogStore {

    /**
     * 追加一条消息并返回其单调位点；位点不复用。
     */
    long append(TopicPartition tp, String messageId, String payload);

    /**
     * 读取 [fromOffset, toOffset) 的原始行，offset 对应“起始偏移+行序号”。
     */
    List<LogEntry> read(TopicPartition tp, long fromOffset, long toOffset);

    /**
     * 起始偏移（物理日志被裁剪后大于 0）。
     */
    long baseOffset(TopicPartition tp);

    /**
     * 下一条消息将分配到的位点。
     */
    long nextOffset(TopicPartition tp);

    /**
     * 当前物理日志中的消息条数。
     */
    long size(TopicPartition tp);

    /**
     * 物理丢弃 offset &lt; truncateBefore 的行（仅允许用于已确认数据）。
     */
    void truncateBefore(TopicPartition tp, long truncateBefore);

    /**
     * 读取已确认位点水位（该位点之前的消息均已确认）；无记录时返回分区起始位点。
     */
    long loadCommittedOffset(TopicPartition tp);

    /**
     * 持久化已确认位点水位，位点单调推进（传入更小的值时保留较大值）。
     */
    void saveCommittedOffset(TopicPartition tp, long committedOffset);

    /**
     * 一条日志原始记录。
     */
    record LogEntry(long offset, String messageId, String payload, long createdAtMs) {
    }
}
