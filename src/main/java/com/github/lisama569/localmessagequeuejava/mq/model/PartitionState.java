package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 消费者组在单个分区上的可恢复状态：已确认位点 + 队头未确认消息。
 */
public record PartitionState(long committedOffset, InFlight head) {

    public static PartitionState init(long startOffset) {
        return new PartitionState(startOffset, null);
    }

    public boolean hasHead() {
        return head != null;
    }
}
