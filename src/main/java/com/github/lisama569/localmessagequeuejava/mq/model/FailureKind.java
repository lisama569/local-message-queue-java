package com.github.lisama569.localmessagequeuejava.mq.model;

/** 失败分类：可重试 / 不可重试 / 重试耗尽。 */
public enum FailureKind {
    RETRYABLE,
    NON_RETRYABLE,
    RETRIES_EXHAUSTED
}
