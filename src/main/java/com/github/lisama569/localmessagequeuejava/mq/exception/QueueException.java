package com.github.lisama569.localmessagequeuejava.mq.exception;

/** 队列相关异常基类。 */
public class QueueException extends RuntimeException {
    public QueueException(String message) { super(message); }
    public QueueException(String message, Throwable cause) { super(message, cause); }
}
