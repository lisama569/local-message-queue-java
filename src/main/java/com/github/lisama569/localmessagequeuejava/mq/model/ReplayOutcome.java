package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 死信重放结果。deduplicated=true 表示命中已确认去重，未产生副作用。
 */
public record ReplayOutcome(String messageId, long offset, boolean deduplicated) {
}
