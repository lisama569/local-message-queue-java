package com.github.lisama569.localmessagequeuejava.mq.model;

import java.util.Objects;

/**
 * 队列中的一条消息。
 *
 * <p>消息一旦写入分区日志即不可变：重试使用同一条消息对象；死信保留其原始载荷；
 * 死信重放会产生一条<b>新日志记录、新位点</b>，但复用原消息标识并置
 * {@code replay=true}——去重账本按消息标识去重，因此重放投递不会绕过重复消费约束。</p>
 */
public final class Message {

    private final String id;
    private final String topic;
    private final int partition;
    private final long offset;
    private final String payload;
    private final boolean replay;

    public Message(String id, String topic, int partition, long offset, String payload) {
        this(id, topic, partition, offset, payload, false);
    }

    public Message(String id, String topic, int partition, long offset, String payload, boolean replay) {
        this.id = Objects.requireNonNull(id, "message id");
        this.topic = Objects.requireNonNull(topic, "topic");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0");
        }
        this.partition = partition;
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        this.offset = offset;
        this.payload = Objects.requireNonNull(payload, "payload");
        this.replay = replay;
    }

    /** 消息标识，全局唯一，去重账本与死信重放均以它为准。 */
    public String getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    /** 分区内单调递增位点，从 0 开始。 */
    public long getOffset() {
        return offset;
    }

    /** 原始载荷，死信与重放均保留。 */
    public String getPayload() {
        return payload;
    }

    /** 是否为死信重放产生的投递（复用原消息标识）。 */
    public boolean isReplay() {
        return replay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Message message)) {
            return false;
        }
        return partition == message.partition
                && offset == message.offset
                && replay == message.replay
                && id.equals(message.id)
                && topic.equals(message.topic)
                && payload.equals(message.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, topic, partition, offset, payload, replay);
    }

    @Override
    public String toString() {
        return "Message{id='" + id + "', topic='" + topic + "', partition=" + partition
                + ", offset=" + offset + (replay ? ", replay=true" : "")
                + ", payload='" + payload + "'}";
    }
}
