package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 不可变消息。offset 由本地存储在追加时单调分配，不回收复用。
 */
public record Message(String id, String topic, int partition, long offset, String payload, long createdAtMs) {
}
