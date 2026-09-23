package com.github.lisama569.localmessagequeuejava.mq.model;

import java.time.Duration;
import java.util.Objects;

/**
 * 本地消息代理配置。
 *
 * <ul>
 *   <li>{@code backoffBase}：退避基数，第 n 次重试等待 {@code backoffBase * 2^(n-1)}；</li>
 *   <li>{@code maxAttempts}：最大尝试次数（含首次），超过后进入死信，分类为重试耗尽；</li>
 *   <li>{@code partitionPrefetch}：每个分区允许的未确认消息数（积压容量边界）；</li>
 *   <li>{@code shutdownAwait}：优雅关停时等待在途消息处理完毕的最长时间；</li>
 *   <li>{@code handleTimeout}：单条消息处理超时，超时按可重试失败处理。</li>
 * </ul>
 */
public final class BrokerConfig {

    private final Duration backoffBase;
    private final int maxAttempts;
    private final int partitionPrefetch;
    private final Duration shutdownAwait;
    private final Duration handleTimeout;

    public BrokerConfig(Duration backoffBase, int maxAttempts, int partitionPrefetch,
                        Duration shutdownAwait, Duration handleTimeout) {
        this.backoffBase = Objects.requireNonNull(backoffBase);
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
        if (partitionPrefetch < 1) {
            throw new IllegalArgumentException("partitionPrefetch must be >= 1");
        }
        this.partitionPrefetch = partitionPrefetch;
        this.shutdownAwait = Objects.requireNonNull(shutdownAwait);
        this.handleTimeout = Objects.requireNonNull(handleTimeout);
    }

    /** 测试友好的默认配置：退避 20ms、最多 3 次、积压 16、关停等待 5s、处理超时 2s。 */
    public static BrokerConfig fast() {
        return new BrokerConfig(Duration.ofMillis(20), 3, 16,
                Duration.ofSeconds(5), Duration.ofSeconds(2));
    }

    public Duration getBackoffBase() {
        return backoffBase;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public int getPartitionPrefetch() {
        return partitionPrefetch;
    }

    public Duration getShutdownAwait() {
        return shutdownAwait;
    }

    public Duration getHandleTimeout() {
        return handleTimeout;
    }
}
