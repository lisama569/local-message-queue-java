package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 死信记录：保留原始载荷与明确的失败原因。
 */
public record DeadLetterRecord(String messageId, String topic, int partition, long offset, String payload,
                               FailureType failureType, String errorMessage, int attempts, long failedAtMs) {
}
