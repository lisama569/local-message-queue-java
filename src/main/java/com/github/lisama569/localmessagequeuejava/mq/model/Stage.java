package com.github.lisama569.localmessagequeuejava.mq.model;

/**
 * 分区队头消息的处理阶段。
 */
public enum Stage {
    /** 已投递给处理器，处理中 */
    IN_FLIGHT,
    /** 可重试失败后退避等待中 */
    RETRY_WAIT
}
