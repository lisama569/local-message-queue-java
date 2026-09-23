package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 拒绝接收新消息/任务的原因，必须与失败分类明确区分。
 */
public enum RejectReason {
    /** 分区积压达到容量上限 */
    QUEUE_FULL,
    /** 正在优雅关停，不再接受新任务 */
    SHUTTING_DOWN
}
