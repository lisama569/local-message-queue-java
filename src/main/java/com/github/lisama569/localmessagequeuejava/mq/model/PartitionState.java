package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 消费者组在单个分区上的可恢复状态：已确认位点 + 队头未确认消息 + 暂停标记。
 */
public record PartitionState(long committedOffset, InFlight head, boolean paused) {

    /** 兼容旧状态格式与无暂停场景的构造器。 */
    public PartitionState(long committedOffset, InFlight head) {
        this(committedOffset, head, false);
    }

    public static PartitionState init(long startOffset) {
        return new PartitionState(startOffset, null, false);
    }

    public boolean hasHead() {
        return head != null;
    }

    public PartitionState withPaused(boolean value) {
        return new PartitionState(committedOffset, head, value);
    }
}
