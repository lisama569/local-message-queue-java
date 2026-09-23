package com.github.lisama569.localmessagequeuejava.mq.exception;

import com.github.lisama569.localmessagequeuejava.mq.model.RejectReason;

/**
 * 因容量边界或关停而明确拒绝，携带可区分的拒绝原因。
 */
public class MessageRejectedException extends RuntimeException {
    private final RejectReason reason;

    public MessageRejectedException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
