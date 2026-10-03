package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 消费者组在单个分区上的可恢复状态：已确认位点 + 队头未确认消息 + 暂停标志。
 *
 * @param paused 分区是否处于暂停投递状态（持久化，热替换/重启后保持）
 */
public record PartitionState(long committedOffset, InFlight head, boolean paused) {

    /** 兼容旧状态文件（无 paused 字段）的便捷构造：默认未暂停。 */
    public PartitionState(long committedOffset, InFlight head) {
        this(committedOffset, head, false);
    }

    public static PartitionState init(long startOffset) {
        return new PartitionState(startOffset, null, false);
    }

    public boolean hasHead() {
        return head != null;
    }

    public PartitionState withPaused(boolean newPaused) {
        return new PartitionState(committedOffset, head, newPaused);
    }
}
