package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 分区中尚未确认的队头消息状态（每分区同一时刻至多一条）。
 */
public record InFlight(long offset, String messageId, int attempts, Stage stage, long nextRetryAtMs,
                       String lastError) {
}
