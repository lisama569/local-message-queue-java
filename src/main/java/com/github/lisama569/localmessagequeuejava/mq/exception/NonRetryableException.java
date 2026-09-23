package com.github.lisama569.localmessagequeuejava.mq.exception;

/**
 * 不可重试失败：立即进入死信，不进入退避。
 */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
