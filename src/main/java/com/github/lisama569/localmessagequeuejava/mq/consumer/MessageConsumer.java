package com.github.lisama569.localmessagequeuejava.mq.consumer;

import com.github.lisama569.localmessagequeuejava.mq.exception.NonRetryableException;
import com.github.lisama569.localmessagequeuejava.mq.handler.MessageHandler;
import com.github.lisama569.localmessagequeuejava.mq.model.*;
import com.github.lisama569.localmessagequeuejava.mq.queue.MessageQueue;
import com.github.lisama569.localmessagequeuejava.mq.store.DeadLetterStore;
import com.github.lisama569.localmessagequeuejava.mq.store.GroupStateStore;
import com.github.lisama569.localmessagequeuejava.mq.store.ProcessedIdStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * 消费者编排：
 * <ul>
 *   <li>同一分区按位点顺序处理，不同分区由独立 worker 并发推进；</li>
 *   <li>每次状态迁移（投递/确认/退避/死信）先落盘再推进，进程中断后可恢复；</li>
 *   <li>失败分类：NonRetryableException 立即死信；达到最大尝试次数死信(重试耗尽)；其余退避重试；</li>
 *   <li>确认前查已确认集合，重复投递/重放不产生重复副作用；</li>
 *   <li>优雅关停：停止拉取新任务，等待在途处理完成并落盘。</li>
 * </ul>
 */
public class MessageConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    private final String group;
    private final MessageQueue queue;
    private final MessageHandler handler;
    private final GroupStateStore groupStateStore;
    private final ProcessedIdStore processedIdStore;
    private final DeadLetterStore deadLetterStore;
    private final ConsumerOptions options;

    private final Object stateLock = new Object();
    private final Map<TopicPartition, PartitionState> states = new HashMap<>();
    private final Set<TopicPartition> subscriptions = new CopyOnWriteArraySet<>();
    private final Map<TopicPartition, ExecutorService> workers = new HashMap<>();
    private final ExecutorService handlerExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "mq-handler-" + UUID.randomUUID().toString().substring(0, 8));
        t.setDaemon(true);
        return t;
    });

    private volatile boolean running;
    private Thread coordinator;

    public MessageConsumer(String group, MessageQueue queue, MessageHandler handler,
                           GroupStateStore groupStateStore, ProcessedIdStore processedIdStore,
                           DeadLetterStore deadLetterStore, ConsumerOptions options) {
        this.group = group;
        this.queue = queue;
        this.handler = handler;
        this.groupStateStore = groupStateStore;
        this.processedIdStore = processedIdStore;
        this.deadLetterStore = deadLetterStore;
        this.options = options;
    }

    public void subscribe(Collection<TopicPartition> partitions) {
        subscriptions.addAll(partitions);
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        synchronized (stateLock) {
            states.putAll(groupStateStore.loadAll());
            // 恢复：进程中断时处于 IN_FLIGHT 的队头按可重试失败处理，进入退避等待
            for (TopicPartition tp : subscriptions) {
                PartitionState st = states.get(tp);
                if (st == null) {
                    st = PartitionState.init(queue.watermark(tp, false).committedOffset());
                    states.put(tp, st);
                }
                if (st.hasHead() && st.head().stage() == Stage.IN_FLIGHT) {
                    InFlight h = st.head();
                    InFlight recovered = new InFlight(h.offset(), h.messageId(), h.attempts() + 1,
                            Stage.RETRY_WAIT, System.currentTimeMillis() + backoffMs(h.attempts() + 1),
                            "进程中断恢复，按可重试失败重新调度");
                    states.put(tp, new PartitionState(st.committedOffset(), recovered, st.paused()));
                    log.info("[恢复] group={} partition={} offset={} messageId={} 判定=IN_FLIGHT中断→退避重试 attempts={}",
                            group, tp.key(), h.offset(), h.messageId(), recovered.attempts());
                }
            }
            persist();
        }
        running = true;
        coordinator = new Thread(this::coordinateLoop, "mq-coordinator-" + group);
        coordinator.setDaemon(true);
        coordinator.start();
        log.info("[启动] group={} 订阅分区={}", group, subscriptions);
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * 按分区暂停投递：已在途的消息继续处理完成，但不再拉取新消息、不触发退避到期重投；
     * 未确认队头、确认位点与分区内顺序原样保留。重复暂停结果稳定（幂等）。
     */
    public void pause(TopicPartition tp) {
        synchronized (stateLock) {
            PartitionState st = states.get(tp);
            if (st == null) {
                st = PartitionState.init(queue.watermark(tp, false).committedOffset());
                states.put(tp, st);
            }
            if (st.paused()) {
                return;
            }
            states.put(tp, st.withPaused(true));
            persist();
            log.info("[暂停] group={} partition={} committedOffset={} headOffset={} 判定=停止新投递，在途继续处理完成",
                    group, tp.key(), st.committedOffset(), st.hasHead() ? st.head().offset() : null);
        }
    }

    /**
     * 恢复分区投递：从暂停前保留的未确认位置继续，不跳过任何消息。重复恢复结果稳定（幂等）。
     */
    public void resume(TopicPartition tp) {
        synchronized (stateLock) {
            PartitionState st = states.get(tp);
            boolean wasPaused = st != null && st.paused();
            if (st == null) {
                st = PartitionState.init(queue.watermark(tp, false).committedOffset());
            }
            states.put(tp, st.withPaused(false));
            persist();
            if (wasPaused) {
                log.info("[恢复] group={} partition={} committedOffset={} headOffset={} 判定=从暂停位置继续投递",
                        group, tp.key(), st.committedOffset(),
                        st.hasHead() ? st.head().offset() : null);
            }
        }
    }

    public boolean isPaused(TopicPartition tp) {
        synchronized (stateLock) {
            PartitionState st = states.get(tp);
            return st != null && st.paused();
        }
    }

    private void coordinateLoop() {
        while (running) {
            try {
                for (TopicPartition tp : subscriptions) {
                    maybeDispatch(tp);
                }
                Thread.sleep(options.getPollInterval().toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("[调度异常] group={} {}", group, e.getMessage(), e);
            }
        }
    }

    private void maybeDispatch(TopicPartition tp) {
        synchronized (stateLock) {
            PartitionState st = states.get(tp);
            if (st == null) {
                return;
            }
            if (st.paused()) {
                // 暂停期间不投递新消息，也不触发退避到期重投；在途状态与顺序原样保留
                return;
            }
            if (!st.hasHead()) {
                long fetchOffset = Math.max(st.committedOffset(),
                        queue.watermark(tp, false).committedOffset());
                Message msg = queue.fetch(tp, fetchOffset);
                if (msg == null) {
                    return;
                }
                InFlight head = new InFlight(msg.offset(), msg.id(), 0, Stage.IN_FLIGHT, 0, null);
                states.put(tp, new PartitionState(st.committedOffset(), head, st.paused()));
                persist();
                log.info("[投递] group={} partition={} offset={} messageId={} 判定=新消息入队头",
                        group, tp.key(), msg.offset(), msg.id());
                submit(tp);
            } else if (st.head().stage() == Stage.RETRY_WAIT
                    && System.currentTimeMillis() >= st.head().nextRetryAtMs()) {
                InFlight h = st.head();
                states.put(tp, new PartitionState(st.committedOffset(),
                        new InFlight(h.offset(), h.messageId(), h.attempts(), Stage.IN_FLIGHT, 0, h.lastError()),
                        st.paused()));
                persist();
                log.info("[投递] group={} partition={} offset={} messageId={} 判定=退避到期重投 attempts={}",
                        group, tp.key(), h.offset(), h.messageId(), h.attempts());
                submit(tp);
            }
        }
    }

    private void submit(TopicPartition tp) {
        ExecutorService worker = workers.computeIfAbsent(tp, k -> {
            ThreadFactory tf = r -> {
                Thread t = new Thread(r, "mq-worker-" + group + "-" + k.key());
                t.setDaemon(true);
                return t;
            };
            return Executors.newSingleThreadExecutor(tf);
        });
        worker.submit(() -> processHead(tp));
    }

    private void processHead(TopicPartition tp) {
        InFlight head;
        long committed;
        synchronized (stateLock) {
            PartitionState st = states.get(tp);
            if (st == null || !st.hasHead() || st.head().stage() != Stage.IN_FLIGHT) {
                return;
            }
            head = st.head();
            committed = st.committedOffset();
        }
        Message msg = queue.fetch(tp, head.offset());
        if (msg == null) {
            // 日志被裁剪等异常情况：跳过该位点，避免卡死
            synchronized (stateLock) {
                PartitionState cur = states.get(tp);
                boolean paused = cur != null && cur.paused();
                commitState(tp, new PartitionState(head.offset() + 1, null, paused));
            }
            log.warn("[跳过] group={} partition={} offset={} messageId={} 判定=位点无数据直接确认",
                    group, tp.key(), head.offset(), head.messageId());
            return;
        }
        // 重复投递去重：已确认过的消息不再触发副作用
        if (processedIdStore.contains(msg.id())) {
            synchronized (stateLock) {
                PartitionState cur = states.get(tp);
                boolean paused = cur != null && cur.paused();
                commitState(tp, new PartitionState(head.offset() + 1, null, paused));
            }
            log.info("[去重] group={} partition={} offset={} messageId={} 判定=已确认过，跳过副作用直接确认",
                    group, tp.key(), head.offset(), msg.id());
            return;
        }
        try {
            invokeWithTimeout(msg);
            synchronized (stateLock) {
                processedIdStore.add(msg.id());
                PartitionState cur = states.get(tp);
                boolean paused = cur != null && cur.paused();
                commitState(tp, new PartitionState(head.offset() + 1, null, paused));
            }
            log.info("[确认] group={} partition={} offset={} messageId={} 判定=处理成功",
                    group, tp.key(), head.offset(), msg.id());
        } catch (Throwable t) {
            onFailure(tp, msg, head, t);
        }
    }

    /**
     * 提交新状态：先推进队列已确认水位（立即释放容量），再落盘消费组状态，
     * 最后按配置物理裁剪日志。调用方必须持有 stateLock。
     */
    private void commitState(TopicPartition tp, PartitionState next) {
        queue.markCommitted(tp, next.committedOffset());
        states.put(tp, next);
        persist();
        releaseCapacity(tp);
    }

    private void invokeWithTimeout(Message msg) throws Throwable {
        Future<?> future = handlerExecutor.submit(() -> {
            try {
                handler.handle(msg);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
        try {
            if (options.getProcessingTimeout().isZero()) {
                future.get();
            } else {
                future.get(options.getProcessingTimeout().toMillis(), TimeUnit.MILLISECONDS);
            }
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RetryableTimeout("处理超时 " + options.getProcessingTimeout().toMillis() + "ms");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            // 解包提交任务时的包装异常，保留业务原始异常用于失败分类
            while (cause instanceof CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            throw cause;
        }
    }

    private void onFailure(TopicPartition tp, Message msg, InFlight head, Throwable t) {
        int attempts = head.attempts() + 1;
        String error = t.getClass().getSimpleName() + ": " + String.valueOf(t.getMessage());
        synchronized (stateLock) {
            PartitionState st = states.get(tp);
            boolean paused = st != null && st.paused();
            if (t instanceof NonRetryableException) {
                deadLetter(tp, msg, attempts, FailureType.NON_RETRYABLE, error);
                commitState(tp, new PartitionState(head.offset() + 1, null, paused));
                log.info("[死信] group={} partition={} offset={} messageId={} 判定=不可重试失败 原因={}",
                        group, tp.key(), head.offset(), msg.id(), error);
            } else if (attempts >= options.getMaxAttempts()) {
                deadLetter(tp, msg, attempts, FailureType.RETRY_EXHAUSTED, error);
                commitState(tp, new PartitionState(head.offset() + 1, null, paused));
                log.info("[死信] group={} partition={} offset={} messageId={} 判定=重试耗尽 attempts={} 原因={}",
                        group, tp.key(), head.offset(), msg.id(), attempts, error);
            } else {
                long retryAt = System.currentTimeMillis() + backoffMs(attempts);
                states.put(tp, new PartitionState(st.committedOffset(),
                        new InFlight(head.offset(), msg.id(), attempts, Stage.RETRY_WAIT, retryAt, error), paused));
                persist();
                log.info("[退避] group={} partition={} offset={} messageId={} 判定=可重试失败 attempts={} 原因={}",
                        group, tp.key(), head.offset(), msg.id(), attempts, error);
            }
        }
    }

    private void deadLetter(TopicPartition tp, Message msg, int attempts, FailureType type, String error) {
        deadLetterStore.add(new DeadLetterRecord(msg.id(), msg.topic(), msg.partition(), msg.offset(),
                msg.payload(), type, error, attempts, System.currentTimeMillis()));
    }

    private long backoffMs(int attempts) {
        double delay = options.getInitialBackoff().toMillis()
                * Math.pow(options.getBackoffMultiplier(), Math.max(0, attempts - 1));
        return (long) Math.min(options.getMaxBackoff().toMillis(), delay);
    }

    private void persist() {
        groupStateStore.saveAll(states);
    }

    /**
     * 确认后物理裁剪已提交日志以释放分区容量（可选，单消费者组场景）。
     */
    private void releaseCapacity(TopicPartition tp) {
        if (!options.isTruncateCommittedLog()) {
            return;
        }
        PartitionState st = states.get(tp);
        if (st != null) {
            queue.truncateBefore(tp, st.committedOffset());
        }
    }

    /**
     * 优雅关停：停止拉取新任务，等待在途处理完成（状态已逐步落盘），超时后强制中断。
     */
    @Override
    public synchronized void close() {
        if (!running) {
            return;
        }
        running = false;
        coordinator.interrupt();
        try {
            coordinator.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long graceMs = Math.max(5000, options.getProcessingTimeout().toMillis() + 2000);
        synchronized (stateLock) {
            workers.values().forEach(ExecutorService::shutdown);
        }
        long deadline = System.currentTimeMillis() + graceMs;
        for (Map.Entry<TopicPartition, ExecutorService> e : workers.entrySet()) {
            long remain = deadline - System.currentTimeMillis();
            try {
                if (!e.getValue().awaitTermination(Math.max(1, remain), TimeUnit.MILLISECONDS)) {
                    log.warn("[关停] group={} partition={} 判定=在途处理超时，强制中断（状态已落盘可恢复）",
                            group, e.getKey().key());
                    e.getValue().shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                e.getValue().shutdownNow();
            }
        }
        handlerExecutor.shutdownNow();
        synchronized (stateLock) {
            persist();
        }
        log.info("[关停] group={} 判定=优雅关停完成，状态已落盘", group);
    }

    public Map<TopicPartition, Long> committedOffsets() {
        synchronized (stateLock) {
            Map<TopicPartition, Long> result = new HashMap<>();
            states.forEach((tp, st) -> result.put(tp, st.committedOffset()));
            return result;
        }
    }

    public List<String> deadLetterIds() {
        return deadLetterStore.findAll().stream().map(DeadLetterRecord::messageId).toList();
    }

    /** 处理超时的内部标记，按可重试失败分类 */
    private static final class RetryableTimeout extends Exception {
        RetryableTimeout(String message) {
            super(message);
        }
    }
}
