package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 分区积压与容量水位：反映当前尚未确认的消息条数与剩余容量。
 */
public record PartitionWatermark(String topic, int partition, long backlog, long capacity, long remaining) {

    public String key() {
        return topic + "#" + partition;
    }
}
