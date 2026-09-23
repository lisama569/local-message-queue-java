package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.exception.BacklogCapacityExceededException;
import com.github.lisama569.localmessagequeuejava.mq.exception.BrokerShutdownException;
import com.github.lisama569.localmessagequeuejava.mq.exception.QueueException;
import com.github.lisama569.localmessagequeuejava.mq.handler.MessageHandler;
import com.github.lisama569.localmessagequeuejava.mq.model.BrokerConfig;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.storage.DeadLetterStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地消息队列代理（无任何外部中间件 / 网络 / 凭据依赖）。
 *
 * <p>一个 broker 承载一个主题的多个分区；每个分区有独立的追加日志与状态文件，
 * 由一个 {@link PartitionConsumer} 工作线程串行消费——因此<b>同分区严格有序、
 * 分区间并发推进</b>。重启后用同一数据目录构造 broker 即恢复全部未确认状态，
 * 语义上等价于「同一消费者组在成员变动后重新接管分区」。</p>
 *
 * <p>两类拒绝明确可区分：</p>
 * <ul>
 *   <li>{@link BacklogCapacityExceededException}：分区未确认积压达到
 *       {@code partitionPrefetch} 容量边界，生产被拒绝（背压）；</li>
 *   <li>{@link BrokerShutdownException}：优雅关停期间拒绝新任务。</li>
 * </ul>
 */
public final class LocalMessageBroker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LocalMessageBroker.class);

    private final Path dataDir;
    private final String topic;
    private final int partitionCount;
    private final BrokerConfig config;

    private final List<PartitionConsumer> consumers = new ArrayList<>();
    private final DeadLetterStore deadLetterStore;
    private final ExecutorService handlerExecutor;

    private final Object lifecycleLock = new Object();
    private volatile boolean started;
    private volatile boolean stopping;

    public LocalMessageBroker(Path dataDir, String topic, int partitionCount,
                              BrokerConfig config, MessageHandler handler) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1");
        }
        this.dataDir = dataDir;
        this.topic = topic;
        this.partitionCount = partitionCount;
        this.config = config;
        this.deadLetterStore = new DeadLetterStore(dataDir);
        ThreadFactory tf = new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "mq-handler-" + seq.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };
        this.handlerExecutor = Executors.newCachedThreadPool(tf);
        for (int p = 0; p < partitionCount; p++) {
            consumers.add(new PartitionConsumer(dataDir, topic, p, config, handler,
                    handlerExecutor, this::persistDeadLetter));
        }
    }

    /** 打开全部存储并启动所有分区消费者。 */
    public void start() {
        synchronized (lifecycleLock) {
            if (started) {
                return;
            }
            try {
                Files.createDirectories(dataDir);
                deadLetterStore.open();
                for (PartitionConsumer c : consumers) {
                    c.start();
                }
            } catch (IOException e) {
                throw new QueueException("failed to start broker in " + dataDir, e);
            }
            started = true;
            log.info("broker-started topic={} partitions={} dataDir={}", topic, partitionCount, dataDir);
        }
    }

    /** 以轮询方式生产，自动生成消息标识。 */
    public Message send(String payload) {
        return send(payload, UUID.randomUUID().toString());
    }

    /**
     * 生产一条消息。路由：{@code Math.floorMod(id.hashCode(), partitionCount)}，
     * 同一消息标识稳定落到同一分区。
     *
     * @throws BacklogCapacityExceededException 目标分区积压达到容量边界
     * @throws BrokerShutdownException broker 正在关停或已关闭
     */
    public Message send(String payload, String messageId) {
        int partition = partitionFor(messageId);
        return sendToPartition(partition, payload, messageId, false);
    }

    /** 显式指定分区生产。 */
    public Message sendToPartition(int partition, String payload, String messageId) {
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException("partition out of range: " + partition);
        }
        return sendToPartition(partition, payload, messageId, false);
    }

    private Message sendToPartition(int partition, String payload, String messageId, boolean replay) {
        if (!started || stopping) {
            throw new BrokerShutdownException("broker is " + (stopping ? "shutting down" : "not started")
                    + "; topic=" + topic + " partition=" + partition + " messageId=" + messageId);
        }
        PartitionConsumer consumer = consumers.get(partition);
        long backlog = consumer.backlog();
        if (backlog >= config.getPartitionPrefetch()) {
            throw new BacklogCapacityExceededException(
                    "partition backlog " + backlog + " reached capacity " + config.getPartitionPrefetch()
                            + "; topic=" + topic + " partition=" + partition + " messageId=" + messageId);
        }
        try {
            return consumer.appendRecord(messageId, payload, replay);
        } catch (IOException e) {
            throw new QueueException("failed to append message", e);
        }
    }

    private int partitionFor(String messageId) {
        return Math.floorMod(messageId.hashCode(), partitionCount);
    }

    /** 当前死信列表（从磁盘读取，含重启恢复的死信）。 */
    public List<DeadLetterRecord> deadLetters() {
        try {
            return deadLetterStore.loadAll();
        } catch (IOException e) {
            throw new QueueException("failed to load dead letters", e);
        }
    }

    /**
     * 按消息标识重放一条死信：作为<b>新日志记录</b>追加回原分区（位点全新），
     * 复用原消息标识并置 replay 标记。去重账本保证：原消息若曾成功产生副作用，
     * 重放不会再次产生；重放记录本身的终态也会持久化，崩溃重启后不重放第二遍。
     */
    public void replayDeadLetter(String messageId) {
        List<DeadLetterRecord> all = deadLetters();
        for (DeadLetterRecord record : all) {
            if (record.getOriginalMessage().getId().equals(messageId)) {
                Message original = record.getOriginalMessage();
                sendToPartition(original.getPartition(), original.getPayload(),
                        original.getId(), true);
                log.info("deadletter-replay-requested topic={} partition={} messageId={} originalOffset={} basis=explicitReplay",
                        topic, original.getPartition(), messageId, original.getOffset());
                return;
            }
        }
        throw new QueueException("dead letter not found: " + messageId);
    }

    /** 重放磁盘上全部死信（按死信记录顺序）。 */
    public void replayAllDeadLetters() {
        for (DeadLetterRecord record : deadLetters()) {
            replayDeadLetter(record.getOriginalMessage().getId());
        }
    }

    /** 等待所有分区积压清零（未确认消息全部到达终态）。 */
    public boolean awaitDrained(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            long total = 0L;
            for (PartitionConsumer c : consumers) {
                total += c.backlog();
            }
            if (total == 0L) {
                return true;
            }
            Thread.sleep(10L);
        }
        return false;
    }

    /** 指定分区未确认消息数。 */
    public long backlog(int partition) {
        return consumers.get(partition).backlog();
    }

    /**
     * 优雅关停：先关门（拒绝一切新生产/重放），再通知各分区停止取新消息，
     * 等待在途消息跑完并持久化确认/重试/死信状态，最后关闭处理器线程池。
     */
    public void shutdown() {
        synchronized (lifecycleLock) {
            if (!started || stopping) {
                stopping = true;
                return;
            }
            stopping = true;
        }
        log.info("broker-shutdown-begin topic={} partitions={} basis=gracefulShutdown", topic, partitionCount);
        for (PartitionConsumer c : consumers) {
            c.requestStop();
        }
        for (PartitionConsumer c : consumers) {
            try {
                c.awaitStop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        handlerExecutor.shutdown();
        try {
            if (!handlerExecutor.awaitTermination(config.getShutdownAwait().toMillis(), TimeUnit.MILLISECONDS)) {
                handlerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            handlerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        synchronized (lifecycleLock) {
            started = false;
        }
        log.info("broker-shutdown-complete topic={} partitions={} basis=gracefulShutdown", topic, partitionCount);
    }

    @Override
    public void close() {
        shutdown();
    }

    private void persistDeadLetter(DeadLetterRecord record) {
        try {
            deadLetterStore.append(record);
        } catch (IOException e) {
            throw new QueueException("failed to persist dead letter: " + record, e);
        }
    }
}
