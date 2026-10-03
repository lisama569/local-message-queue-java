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
                    st = PartitionState.init(queue.nextOffset(tp) - queue.backlog(tp));
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
                // 接管分区时按已确认位点重建未确认计数，使容量水位与消费进度一致
                queue.reconcileUnacked(tp, states.get(tp).committedOffset());
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
            // 暂停闸门：不拉取新消息，也不重投退避到期消息；队头状态与顺序原样保留
            if (st.paused()) {
                return;
            }
            if (!st.hasHead()) {
                long fetchOffset = Math.max(st.committedOffset(), queue.nextOffset(tp) - queue.backlog(tp));
                Message msg = queue.fetch(tp, fetchOffset);
                if (msg == null) {
                    return;
                }
                InFlight head = new InFlight(msg.offset(), msg.id(), 0, Stage.IN_FLIGHT, 0, null);
                states.put(tp, new PartitionState(st.committedOffset(), head));
                persist();
                log.info("[投递] group={} partition={} offset={} messageId={} 判定=新消息入队头",
                        group, tp.key(), msg.offset(), msg.id());
                submit(tp);
            } else if (st.head().stage() == Stage.RETRY_WAIT
                    && System.currentTimeMillis() >= st.head().nextRetryAtMs()) {
                InFlight h = st.head();
                states.put(tp, new PartitionState(st.committedOffset(),
                        new InFlight(h.offset(), h.messageId(), h.attempts(), Stage.IN_FLIGHT, 0, h.lastError())));
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
            boolean paused;
            synchronized (stateLock) {
                PartitionState st = states.get(tp);
                paused = st.paused();
                states.put(tp, new PartitionState(head.offset() + 1, null, paused));
                persist();
                afterCommit(tp, head.offset() + 1);
            }
            log.warn("[跳过] group={} partition={} offset={} messageId={} 判定=位点无数据直接确认",
                    group, tp.key(), head.offset(), head.messageId());
            return;
        }
        // 重复投递去重：已确认过的消息不再触发副作用
        if (processedIdStore.contains(msg.id())) {
            boolean paused;
            synchronized (stateLock) {
                PartitionState st = states.get(tp);
                paused = st.paused();
                states.put(tp, new PartitionState(head.offset() + 1, null, paused));
                persist();
                afterCommit(tp, head.offset() + 1);
            }
            log.info("[去重] group={} partition={} offset={} messageId={} 判定=已确认过，跳过副作用直接确认",
                    group, tp.key(), head.offset(), msg.id());
            return;
        }
        try {
            invokeWithTimeout(msg);
            boolean paused;
            synchronized (stateLock) {
                PartitionState st = states.get(tp);
                paused = st.paused();
                processedIdStore.add(msg.id());
                states.put(tp, new PartitionState(head.offset() + 1, null, paused));
                persist();
                afterCommit(tp, head.offset() + 1);
            }
            log.info("[确认] group={} partition={} offset={} messageId={} 判定=处理成功",
                    group, tp.key(), head.offset(), msg.id());
        } catch (Throwable t) {
            onFailure(tp, msg, head, t);
        }
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
            if (t instanceof NonRetryableException) {
                deadLetter(tp, msg, attempts, FailureType.NON_RETRYABLE, error);
                states.put(tp, new PartitionState(head.offset() + 1, null, st.paused()));
                persist();
                afterCommit(tp, head.offset() + 1);
                log.info("[死信] group={} partition={} offset={} messageId={} 判定=不可重试失败 原因={}",
                        group, tp.key(), head.offset(), msg.id(), error);
            } else if (attempts >= options.getMaxAttempts()) {
                deadLetter(tp, msg, attempts, FailureType.RETRY_EXHAUSTED, error);
                states.put(tp, new PartitionState(head.offset() + 1, null, st.paused()));
                persist();
                afterCommit(tp, head.offset() + 1);
                log.info("[死信] group={} partition={} offset={} messageId={} 判定=重试耗尽 attempts={} 原因={}",
                        group, tp.key(), head.offset(), msg.id(), attempts, error);
            } else {
                long retryAt = System.currentTimeMillis() + backoffMs(attempts);
                states.put(tp, new PartitionState(st.committedOffset(),
                        new InFlight(head.offset(), msg.id(), attempts, Stage.RETRY_WAIT, retryAt, error),
                        st.paused()));
                log.info("[退避] group={} partition={} offset={} messageId={} 判定=可重试失败 attempts={} 原因={}",
                        group, tp.key(), head.offset(), msg.id(), attempts, error);
            }
            persist();
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
     * 一条消息被确认（成功/去重/死信）后的容量处理：
     * 立即释放 1 个未确认占额；开启裁剪时再物理丢弃已提交历史日志。
     * 必须在持有 stateLock 时调用。
     */
    private void afterCommit(TopicPartition tp, long committedOffset) {
        queue.releaseOne(tp);
        if (options.isTruncateCommittedLog()) {
            queue.truncateBefore(tp, committedOffset);
        }
    }

    /**
     * 暂停某分区投递：不影响在途处理，但不再拉取新消息/重投退避消息；
     * 未确认状态与分区内顺序原样保留。重复调用幂等。
     */
    public boolean pause(TopicPartition tp) {
        synchronized (stateLock) {
            PartitionState st = states.get(tp);
            if (st == null) {
                throw new IllegalArgumentException("订阅中不存在该分区: group=" + group + " " + tp.key());
            }
            if (st.paused()) {
                log.info("[暂停] group={} partition={} 判定=已处于暂停态，重复调用保持不变",
                        group, tp.key());
                return false;
            }
            states.put(tp, st.withPaused(true));
            persist();
            log.info("[暂停] group={} partition={} committed={} head={} 判定=停止投递，未确认状态保留",
                    group, tp.key(), st.committedOffset(),
                    st.hasHead() ? st.head().offset() + "/" + st.head().messageId() : "无");
            return true;
        }
    }

    /**
     * 恢复某分区投递：从暂停前保留的位点/队头继续，不跳过任何消息。重复调用幂等。
     */
    public boolean resume(TopicPartition tp) {
        synchronized (stateLock) {
            PartitionState st = states.get(tp);
            if (st == null) {
                throw new IllegalArgumentException("订阅中不存在该分区: group=" + group + " " + tp.key());
            }
            if (!st.paused()) {
                log.info("[恢复投递] group={} partition={} 判定=本就处于投递态，重复调用保持不变",
                        group, tp.key());
                return false;
            }
            states.put(tp, st.withPaused(false));
            persist();
            log.info("[恢复投递] group={} partition={} committed={} head={} 判定=从原位点继续投递",
                    group, tp.key(), st.committedOffset(),
                    st.hasHead() ? st.head().offset() + "/" + st.head().messageId() : "无");
            return true;
        }
    }

    /** 当前处于暂停态的分区（测试与运维查询）。 */
    public Set<TopicPartition> pausedPartitions() {
        synchronized (stateLock) {
            return states.entrySet().stream().filter(e -> e.getValue().paused())
                    .map(Map.Entry::getKey).collect(java.util.stream.Collectors.toSet());
        }
    }

    /** 订阅的分区集合。 */
    public Set<TopicPartition> subscriptions() {
        return Set.copyOf(subscriptions);
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
