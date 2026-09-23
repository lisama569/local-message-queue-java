package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 失败分类，三者必须可区分。
 */
public enum FailureType {
    /** 可重试失败（含处理超时） */
    RETRYABLE,
    /** 不可重试失败，立即进入死信 */
    NON_RETRYABLE,
    /** 可重试但达到最大尝试次数后耗尽 */
    RETRY_EXHAUSTED
}
