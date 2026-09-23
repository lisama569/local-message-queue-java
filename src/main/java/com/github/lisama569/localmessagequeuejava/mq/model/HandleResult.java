package com.github.lisama569.localmessagequeuejava.mq.model;

import java.util.Objects;

/**
 * 业务处理器对一条消息的判定结果。
 *
 * <ul>
 *   <li>{@link #ack()}：处理成功，框架提交位点并把消息标识写入完成账本；</li>
 *   <li>{@link #retry()} / {@link #retry(String)}：声明为可重试失败，按指数退避重试；</li>
 *   <li>{@link #deadLetter(String)}：声明为不可重试失败，直接进入死信。</li>
 * </ul>
 *
 * <p>处理器直接抛异常时，框架默认按「可重试失败」处理（见消费编排器）。</p>
 */
public final class HandleResult {

    /** 判定类型（仅三种终向）。 */
    public enum Decision { ACK, RETRY, DEAD_LETTER }

    private static final HandleResult ACK = new HandleResult(Decision.ACK, null);

    private final Decision decision;
    private final String reason;

    private HandleResult(Decision decision, String reason) {
        this.decision = Objects.requireNonNull(decision);
        this.reason = reason;
    }

    public static HandleResult ack() {
        return ACK;
    }

    public static HandleResult retry() {
        return new HandleResult(Decision.RETRY, null);
    }

    public static HandleResult retry(String reason) {
        return new HandleResult(Decision.RETRY, reason);
    }

    public static HandleResult deadLetter(String reason) {
        return new HandleResult(Decision.DEAD_LETTER, Objects.requireNonNull(reason, "dead-letter reason"));
    }

    public Decision decision() {
        return decision;
    }

    /** 失败原因，ACK 时为 null。 */
    public String reason() {
        return reason;
    }
}
