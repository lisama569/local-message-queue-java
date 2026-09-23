package com.github.lisama569.localmessagequeuejava.mq.consumer;

import java.time.Duration;

/**
 * 消费者运行参数（退避、最大尝试、超时等）。
 */
public final class ConsumerOptions {

    private Duration initialBackoff = Duration.ofMillis(100);
    private Duration maxBackoff = Duration.ofSeconds(10);
    private double backoffMultiplier = 2.0;
    private int maxAttempts = 5;
    private Duration processingTimeout = Duration.ZERO;
    private Duration pollInterval = Duration.ofMillis(20);
    private long assignmentScanIntervalMs = 100;
    /** 确认后是否物理裁剪已提交日志以释放分区容量（单消费者组场景可开启） */
    private boolean truncateCommittedLog = false;

    public static ConsumerOptions defaults() {
        return new ConsumerOptions();
    }

    public Duration getInitialBackoff() {
        return initialBackoff;
    }

    public ConsumerOptions initialBackoff(Duration v) {
        this.initialBackoff = v;
        return this;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public ConsumerOptions maxBackoff(Duration v) {
        this.maxBackoff = v;
        return this;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public ConsumerOptions backoffMultiplier(double v) {
        this.backoffMultiplier = v;
        return this;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public ConsumerOptions maxAttempts(int v) {
        this.maxAttempts = v;
        return this;
    }

    public Duration getProcessingTimeout() {
        return processingTimeout;
    }

    public ConsumerOptions processingTimeout(Duration v) {
        this.processingTimeout = v;
        return this;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public ConsumerOptions pollInterval(Duration v) {
        this.pollInterval = v;
        return this;
    }

    public long getAssignmentScanIntervalMs() {
        return assignmentScanIntervalMs;
    }

    public ConsumerOptions assignmentScanIntervalMs(long v) {
        this.assignmentScanIntervalMs = v;
        return this;
    }

    public boolean isTruncateCommittedLog() {
        return truncateCommittedLog;
    }

    public ConsumerOptions truncateCommittedLog(boolean v) {
        this.truncateCommittedLog = v;
        return this;
    }
}
