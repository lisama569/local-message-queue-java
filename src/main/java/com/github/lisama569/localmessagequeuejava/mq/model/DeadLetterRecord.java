package com.github.lisama569.localmessagequeuejava.mq.model;

import java.util.Objects;

/**
 * 死信记录。
 *
 * <p>保留：原始消息（含原始载荷）、失败分类、失败原因、累计尝试次数、进入死信的时间。
 * 失败分类明确区分：不可重试失败（首次即判定）与重试耗尽（达到最大次数仍失败）。</p>
 */
public final class DeadLetterRecord {

    private final Message originalMessage;
    private final FailureKind failureKind;
    private final String reason;
    private final int attempts;
    private final long deadLetteredAt;

    public DeadLetterRecord(Message originalMessage, FailureKind failureKind,
                            String reason, int attempts, long deadLetteredAt) {
        this.originalMessage = Objects.requireNonNull(originalMessage);
        this.failureKind = Objects.requireNonNull(failureKind);
        this.reason = reason;
        if (attempts < 1) {
            throw new IllegalArgumentException("attempts must be >= 1");
        }
        this.attempts = attempts;
        this.deadLetteredAt = deadLetteredAt;
    }

    public Message getOriginalMessage() {
        return originalMessage;
    }

    public FailureKind getFailureKind() {
        return failureKind;
    }

    public String getReason() {
        return reason;
    }

    public int getAttempts() {
        return attempts;
    }

    public long getDeadLetteredAt() {
        return deadLetteredAt;
    }

    @Override
    public String toString() {
        return "DeadLetterRecord{message=" + originalMessage
                + ", kind=" + failureKind
                + ", reason='" + reason + '\''
                + ", attempts=" + attempts
                + ", deadLetteredAt=" + deadLetteredAt + '}';
    }
}
