package com.github.lisama569.localmessagequeuejava.mq.exception;

/** 关停期间拒绝新任务（与容量拒绝明确区分）。 */
public class BrokerShutdownException extends QueueException {
    public BrokerShutdownException(String message) { super(message); }
}
