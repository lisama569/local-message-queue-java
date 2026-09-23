package com.github.lisama569.localmessagequeuejava.mq.exception;

/** 分区积压达到容量边界（明确拒绝生产）。 */
public class BacklogCapacityExceededException extends QueueException {
    public BacklogCapacityExceededException(String message) { super(message); }
}
