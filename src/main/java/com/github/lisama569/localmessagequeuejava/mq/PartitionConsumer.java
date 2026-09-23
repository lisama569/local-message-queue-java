package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.handler.MessageHandler;
import com.github.lisama569.localmessagequeuejava.mq.model.BrokerConfig;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.FailureKind;
import com.github.lisama569.localmessagequeuejava.mq.model.HandleResult;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.storage.PartitionLog;
import com.github.lisama569.localmessagequeuejava.mq.storage.PartitionStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * 单个分区的消费、确认与重试编排器。
 *
 * <p>关键保证：</p>
 * <ul>
 *   <li><b>分区内串行</b>：仅一个工作线程，任何时刻只处理一条消息；前一条未到
 *       终态（确认 / 死信）不会推进位点，天然保证同分区既定顺序；不同分区各跑各的线程。</li>
 *   <li><b>崩溃一致</b>：所有「确认 / 待重试 / 死信」决定都先持久化（账本 append+fsync、
 *       状态文件原子替换）再在内存推进；工作中被杀的消息重启后按原位点重投。</li>
 *   <li><b>重复安全</b>：调用业务处理器前先查完成账本；已完成 / 已死信（非重放记录）/
 *       已终态的重放记录一律跳过副作用。</li>
 *   <li><b>优雅关停</b>：{@code requestStop} 后不再取新消息，但当前消息会跑完并落盘。</li>
 * </ul>
 */
final class PartitionConsumer {

    private static final Logger log = LoggerFactory.getLogger(PartitionConsumer.class);

    /** 退避上限 30s，避免指数无限放大。 */
    private static final long MAX_BACKOFF_MS = 30_000L;

    private final String topic;
    private final int partition;
    private final BrokerConfig config;
    private final MessageHandler handler;
    private final ExecutorService handlerExecutor;
    private final Consumer<DeadLetterRecord> deadLetterSink;

    private final PartitionLog partitionLog;
    private final PartitionStateStore stateStore;

    private final Object lock = new Object();
    private boolean started;
    private boolean stopRequested;
    /** 优雅关停的截止时间（绝对毫秒）；在此之前排空已接收积压，之后不再取新消息。 */
    private long stopDeadlineMs;
    private Thread worker;
    private volatile long logEndOffset;

    PartitionConsumer(Path dataDir, String topic, int partition, BrokerConfig config,
                      MessageHandler handler, ExecutorService handlerExecutor,
                      Consumer<DeadLetterRecord> deadLetterSink) {
        this.topic = topic;
        this.partition = partition;
        this.config = config;
        this.handler = handler;
        this.handlerExecutor = handlerExecutor;
        this.deadLetterSink = deadLetterSink;
        this.partitionLog = new PartitionLog(dataDir, topic, partition);
        this.stateStore = new PartitionStateStore(dataDir, topic, partition);
    }

    String topic() {
        return topic;
    }

    int partition() {
        return partition;
    }

    /**
     * 追加（生产）一条消息。broker 在容量/关停校验后调用本方法。
     * @return 落盘后带位点的消息
     */
    Message appendRecord(String id, String payload, boolean replay) throws IOException {
        Message message = partitionLog.append(id, payload, replay);
        synchronized (lock) {
            logEndOffset = partitionLog.getLogEndOffset();
            lock.notifyAll();
        }
        log.info("append topic={} partition={} offset={} messageId={} replay={} decision=APPEND basis=fsynced",
                topic, partition, message.getOffset(), id, replay);
        return message;
    }

    /** 未确认消息数（积压）。 */
    long backlog() {
        return logEndOffset - stateStore.getCommittedOffset();
    }

    void start() throws IOException {
        synchronized (lock) {
            if (started) {
                return;
            }
            partitionLog.open();
            stateStore.open();
            logEndOffset = partitionLog.getLogEndOffset();
            started = true;
            stopRequested = false;
            stopDeadlineMs = 0L;
            worker = new Thread(this::run, "mq-consumer-" + topic + "-p" + partition);
            worker.setDaemon(true);
            worker.start();
        }
    }

    void requestStop() {
        synchronized (lock) {
            if (!stopRequested) {
                stopDeadlineMs = System.currentTimeMillis()
                        + config.getShutdownAwait().toMillis();
            }
            stopRequested = true;
            lock.notifyAll();
        }
    }

    void awaitStop() throws InterruptedException {
        Thread t;
        synchronized (lock) {
            t = worker;
        }
        if (t != null) {
            t.join(config.getShutdownAwait().toMillis());
        }
    }

    private void run() {
        try {
            fastForwardCommittedSuffix();
            PartitionStateStore.PendingRetry pending = stateStore.getPendingRetry();
            long attempt = 0L;
            long readyAt = 0L;
            String pendingReason = null;
            FailureKind pendingKind = null;
            if (pending != null) {
                attempt = pending.attempt();
                readyAt = pending.readyAt();
                pendingReason = pending.reason();
                pendingKind = pending.kind();
            }
            while (true) {
                long cursor;
                Message current;
                synchronized (lock) {
                    cursor = stateStore.getCommittedOffset();
                    boolean backlogRemains = cursor < logEndOffset;
                    boolean retryDueRemains = pending != null
                            && System.currentTimeMillis() >= readyAt;
                    // 等待条件：
                    //  - 运行中：无积压或退避未到 => 等；
                    //  - 关停中：仍在 drain 时限内且有积压（或到点的重试）=> 继续处理；
                    //    超过 drain 时限 / 积压清空 => 退出。
                    while (true) {
                        long now = System.currentTimeMillis();
                        if (!stopRequested) {
                            if (cursor >= logEndOffset
                                    || (pending != null && now < readyAt)) {
                                if (cursor >= logEndOffset) {
                                    lock.wait(50L);
                                } else {
                                    lock.wait(Math.max(1L,
                                            Math.min(readyAt - now, 200L)));
                                }
                                cursor = stateStore.getCommittedOffset();
                                if (pending == null) {
                                    readyAt = 0L;
                                }
                                continue;
                            }
                            break;
                        }
                        // 关停中
                        boolean canDrain = now < stopDeadlineMs
                                && (backlogRemains || retryDueRemains);
                        if (canDrain) {
                            if (pending != null && now < readyAt) {
                                lock.wait(Math.max(1L,
                                        Math.min(Math.min(readyAt - now, stopDeadlineMs - now), 50L)));
                                cursor = stateStore.getCommittedOffset();
                                backlogRemains = cursor < logEndOffset;
                                continue;
                            }
                            break;
                        }
                        log.info("shutdown topic={} partition={} committedOffset={} logEnd={} basis=stopRequested drained={}",
                                topic, partition, cursor, logEndOffset, !backlogRemains);
                        return;
                    }
                    if (pending != null) {
                        current = findMessageAt(pending.offset());
                        if (current == null || System.currentTimeMillis() < readyAt) {
                            continue;
                        }
                    } else {
                        current = findMessageAt(cursor);
                        if (current == null) {
                            continue;
                        }
                    }
                }

                if (pending != null) {
                    log.info("retry-due topic={} partition={} offset={} messageId={} attempt={} replay={} basis=backoffElapsed kind={} reason={}",
                            topic, partition, current.getOffset(), current.getId(), attempt,
                            current.isReplay(), pendingKind, pendingReason);
                }

                // 处理前先判重：重复投递（同一 messageId 的第二条日志记录）、
                // 崩溃在「标记终态之后、提交位点之前」、已死信重发等，均跳过副作用。
                if (isAlreadyTerminal(current)) {
                    if (pending != null) {
                        stateStore.clearPendingRetry();
                    }
                    skipTerminal(current);
                    pending = null;
                    continue;
                }

                int nextAttempt = pending == null ? 1 : (int) attempt;
                Outcome outcome = invokeHandler(current, nextAttempt);
                pending = null;

                switch (outcome.decision()) {
                    case ACK -> handleAck(current);
                    case RETRY -> {
                        PendingRetryDecision d = handleRetry(current, outcome, nextAttempt,
                                current.isReplay());
                        if (d == PendingRetryDecision.DEAD_LETTER_INSTEAD) {
                            handleDeadLetter(current, FailureKind.RETRIES_EXHAUSTED,
                                    outcome.reason(), nextAttempt);
                        } else {
                            pending = stateStore.getPendingRetry();
                            attempt = pending.attempt();
                            readyAt = pending.readyAt();
                            pendingReason = pending.reason();
                            pendingKind = pending.kind();
                        }
                    }
                    case DEAD_LETTER -> handleDeadLetter(current, FailureKind.NON_RETRYABLE,
                            outcome.reason(), nextAttempt);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("worker-interrupted topic={} partition={} basis=interrupt", topic, partition);
        } catch (Throwable t) {
            log.error("worker-fatal topic={} partition={} basis=unexpected state persisted before crash",
                    topic, partition, t);
        }
    }

    /**
     * 启动时把「位点之后、但账本中已完成/已死信」的消息快速跳过提交。
     * 正常运行下不会出现这种空洞；它只用于兼容历史状态或异常中断后的保守恢复。
     */
    private void fastForwardCommittedSuffix() throws IOException {
        List<Message> all = partitionLog.loadAll();
        long cursor = stateStore.getCommittedOffset();
        while (cursor < all.size()) {
            Message m = all.get((int) cursor);
            boolean alreadyTerminal = stateStore.isCompleted(m.getId())
                    || stateStore.isDeadLettered(m.getId())
                    || stateStore.isReplayTerminal(m.getOffset());
            if (!alreadyTerminal) {
                break;
            }
            stateStore.commitOffset(cursor + 1);
            PartitionStateStore.PendingRetry stale = stateStore.getPendingRetry();
            if (stale != null && stale.offset() <= cursor) {
                // 待重试指向的记录已被账本判定为终态（典型：崩溃在落盘与提交之间），作废
                stateStore.clearPendingRetry();
                log.info("recover-clear-pending topic={} partition={} offset={} messageId={} basis=ledgerTerminal",
                        topic, partition, m.getOffset(), m.getId());
            }
            log.info("recover-skip topic={} partition={} offset={} messageId={} replay={} basis=ledgerTerminal",
                    topic, partition, m.getOffset(), m.getId(), m.isReplay());
            cursor++;
        }
    }

    /**
     * 判断一条日志记录是否已无需再执行业务副作用：
     * <ul>
     *   <li>普通记录：messageId 已在完成账本或已死信账本中；</li>
     *   <li>重放记录：对应 messageId 已完成（重放成功过），或该重放位点已走完终态。</li>
     * </ul>
     */
    private boolean isAlreadyTerminal(Message message) {
        if (message.isReplay()) {
            return stateStore.isCompleted(message.getId())
                    || stateStore.isReplayTerminal(message.getOffset());
        }
        return stateStore.isCompleted(message.getId())
                || stateStore.isDeadLettered(message.getId());
    }

    /** 跳过一条已终态记录：重放记录先补终态标记，随后提交位点，不调用 handler。 */
    private void skipTerminal(Message message) throws IOException {
        if (message.isReplay()) {
            stateStore.markReplayTerminal(message.getOffset());
        }
        stateStore.commitOffset(message.getOffset() + 1);
        log.info("dedup-skip topic={} partition={} offset={} messageId={} replay={} decision=SKIP basis=ledgerShowsTerminal",
                topic, partition, message.getOffset(), message.getId(), message.isReplay());
    }

    private Message findMessageAt(long offset) {        try {
            List<Message> all = partitionLog.loadAll();
            if (offset >= 0 && offset < all.size()) {
                return all.get((int) offset);
            }
            return null;
        } catch (IOException e) {
            log.error("find-message-failed topic={} partition={} offset={} basis=ioError",
                    topic, partition, offset, e);
            return null;
        }
    }

    private Outcome invokeHandler(Message message, int attempt) {
        long start = System.currentTimeMillis();
        Future<HandleResult> future = handlerExecutor.submit(() -> handler.handle(message));
        try {
            HandleResult result = future.get(config.getHandleTimeout().toMillis(), TimeUnit.MILLISECONDS);
            long elapsed = System.currentTimeMillis() - start;
            if (result == null) {
                log.info("handle-null topic={} partition={} offset={} messageId={} attempt={} replay={} decision=RETRY basis=handlerReturnedNull elapsedMs={}",
                        topic, partition, message.getOffset(), message.getId(), attempt,
                        message.isReplay(), elapsed);
                return new Outcome(HandleResult.Decision.RETRY, "handler returned null");
            }
            log.info("handle topic={} partition={} offset={} messageId={} attempt={} replay={} decision={} reason={} elapsedMs={}",
                    topic, partition, message.getOffset(), message.getId(), attempt,
                    message.isReplay(), result.decision(), result.reason(), elapsed);
            return new Outcome(result.decision(), result.reason());
        } catch (TimeoutException e) {
            future.cancel(true);
            long elapsed = System.currentTimeMillis() - start;
            log.info("handle-timeout topic={} partition={} offset={} messageId={} attempt={} replay={} decision=RETRY kind=RETRYABLE basis=timeout elapsedMs={}",
                    topic, partition, message.getOffset(), message.getId(), attempt,
                    message.isReplay(), elapsed);
            return new Outcome(HandleResult.Decision.RETRY,
                    "handle timeout after " + elapsed + "ms");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            long elapsed = System.currentTimeMillis() - start;
            String reason = cause.getClass().getSimpleName()
                    + (cause.getMessage() != null ? ": " + cause.getMessage() : "");
            log.info("handle-exception topic={} partition={} offset={} messageId={} attempt={} replay={} decision=RETRY kind=RETRYABLE basis=thrownException elapsedMs={} reason={}",
                    topic, partition, message.getOffset(), message.getId(), attempt,
                    message.isReplay(), elapsed, reason);
            return new Outcome(HandleResult.Decision.RETRY, reason);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void handleAck(Message message) throws IOException {
        if (message.isReplay()) {
            // 重放成功：先记完成账本，再标记重放记录终态，最后提交位点
            stateStore.recordCompleted(message.getId(), message.getOffset());
            stateStore.markReplayTerminal(message.getOffset());
            stateStore.commitOffset(message.getOffset() + 1);
            log.info("ack-replay topic={} partition={} offset={} messageId={} decision=ACK basis=handlerAck+ledgerDedup",
                    topic, partition, message.getOffset(), message.getId());
        } else {
            stateStore.recordCompleted(message.getId(), message.getOffset());
            stateStore.commitOffset(message.getOffset() + 1);
            log.info("ack topic={} partition={} offset={} messageId={} decision=ACK basis=handlerAck",
                    topic, partition, message.getOffset(), message.getId());
        }
    }

    private PendingRetryDecision handleRetry(Message message, Outcome outcome, int attempt,
                                             boolean replay) throws IOException {
        int maxAttempts = config.getMaxAttempts();
        if (attempt >= maxAttempts) {
            log.info("retry-exhausted topic={} partition={} offset={} messageId={} attempt={} replay={} decision=DEAD_LETTER kind=RETRIES_EXHAUSTED basis=attemptReachedMax maxAttempts={} reason={}",
                    topic, partition, message.getOffset(), message.getId(), attempt, replay,
                    maxAttempts, outcome.reason());
            return PendingRetryDecision.DEAD_LETTER_INSTEAD;
        }
        int nextAttempt = attempt + 1;
        long backoffMs = Math.min(
                config.getBackoffBase().toMillis() * (1L << (attempt - 1)),
                MAX_BACKOFF_MS);
        long readyAt = System.currentTimeMillis() + backoffMs;
        stateStore.savePendingRetry(message.getOffset(), nextAttempt, outcome.reason(),
                FailureKind.RETRYABLE, readyAt);
        log.info("retry-scheduled topic={} partition={} offset={} messageId={} attempt={}->{} replay={} decision=RETRY kind=RETRYABLE backoffMs={} basis=retryableFailure reason={}",
                topic, partition, message.getOffset(), message.getId(), attempt, nextAttempt,
                replay, backoffMs, outcome.reason());
        return PendingRetryDecision.WAIT;
    }

    private void handleDeadLetter(Message message, FailureKind kind, String reason, int attempts)
            throws IOException {
        DeadLetterRecord record = new DeadLetterRecord(message, kind, reason, attempts,
                System.currentTimeMillis());
        // 先持久化死信（DLQ + 标记），再提交位点；崩溃只会导致重判，不会漏
        deadLetterSink.accept(record);
        if (message.isReplay()) {
            stateStore.markReplayTerminal(message.getOffset());
            log.info("deadletter-replay topic={} partition={} offset={} messageId={} decision=DEAD_LETTER kind={} attempts={} replay=true basis=replayTerminal reason={}",
                    topic, partition, message.getOffset(), message.getId(), kind, attempts, reason);
        } else {
            stateStore.recordDeadLettered(message.getId());
            log.info("deadletter topic={} partition={} offset={} messageId={} decision=DEAD_LETTER kind={} attempts={} basis=deadletterPersisted reason={}",
                    topic, partition, message.getOffset(), message.getId(), kind, attempts, reason);
        }
        stateStore.commitOffset(message.getOffset() + 1);
    }

    private enum PendingRetryDecision { WAIT, DEAD_LETTER_INSTEAD }

    private record Outcome(HandleResult.Decision decision, String reason) { }
}
