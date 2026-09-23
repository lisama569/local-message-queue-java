package com.github.lisama569.localmessagequeuejava.mq.exception;

/**
 * 可重试失败。未显式分类的其它异常在框架内也按可重试处理。
 */
public class RetryableException extends RuntimeException {
    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
