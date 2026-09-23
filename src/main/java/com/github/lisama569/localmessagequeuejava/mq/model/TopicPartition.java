package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 主题分区坐标。
 */
public record TopicPartition(String topic, int partition) {
    public String key() {
        return topic + "#" + partition;
    }
}
