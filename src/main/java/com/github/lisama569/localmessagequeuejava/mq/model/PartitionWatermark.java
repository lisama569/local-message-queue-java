package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 分区积压/容量水位视图：以"已确认位点"为基准实时反映未确认消息。
 *
 * @param baseOffset       分区起始位点（物理日志裁剪后大于 0，位点不复用）
 * @param nextOffset       下一条新消息将分配到的位点
 * @param committedOffset  已确认位点水位（该位点之前的消息均已确认）
 * @param backlog          当前未确认积压条数 = nextOffset - committedOffset
 * @param capacity         分区容量上限
 * @param remaining        距容量上限的剩余可接收条数 = capacity - backlog
 * @param paused           该分区在被查询消费组视角下是否暂停（无消费组时恒为 false）
 */
public record PartitionWatermark(String topic, int partition,
                                 long baseOffset, long nextOffset, long committedOffset,
                                 long backlog, long capacity, long remaining, boolean paused) {
}
