package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.consumer.ConsumerOptions;
import com.github.lisama569.localmessagequeuejava.mq.exception.RetryableException;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 按分区暂停/恢复投递：
 * <ul>
 *   <li>暂停期间在途消息继续处理完成，但不再投递新消息（含退避到期重投）；</li>
 *   <li>未确认位点与分区内顺序保留，恢复后从原位置继续，不跳消息；</li>
 *   <li>其他分区不受影响、照常并发；暂停/恢复反复调用幂等稳定。</li>
 * </ul>
 */
class PauseResumeTest {

    private static final Logger log = LoggerFactory.getLogger(PauseResumeTest.class);

    @TempDir
    Path dataDir;

    @Test
    void pauseHoldsInFlightAndPositionOthersAdvanceResumeContinues() throws Exception {
        TopicPartition tp0 = new TopicPartition("flow", 0);
        TopicPartition tp1 = new TopicPartition("flow", 1);
        List<Long> offsetsP0 = new CopyOnWriteArrayList<>();
        List<Long> offsetsP1 = new CopyOnWriteArrayList<>();
        CountDownLatch m1Started = new CountDownLatch(1);

        try (MqService service = new MqService(dataDir, 1000)) {
            for (int i = 0; i < 6; i++) {
                service.send("flow", 0, "p0-" + i, "a" + i);
                service.send("flow", 1, "p1-" + i, "b" + i);
            }

            service.registerConsumer("g-flow", msg -> {
                if (msg.partition() == 0) {
                    offsetsP0.add(msg.offset());
                    if (msg.offset() == 1) {
                        m1Started.countDown();
                        log.info("[在途] partition=flow#0 offset=1 messageId=p0-1 判定=暂停瞬间仍在处理，需跑完");
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    log.info("[消费] partition=flow#0 offset={} messageId={}", msg.offset(), msg.id());
                } else {
                    offsetsP1.add(msg.offset());
                }
            }, ConsumerOptions.defaults(), List.of(tp0, tp1));

            // 卡在 m1 在途窗口暂停：m1 必须跑完，但 m2 起不得投递
            assertThat(m1Started.await(10, TimeUnit.SECONDS)).isTrue();
            service.pause("g-flow", tp0);
            log.info("[暂停] partition=flow#0 判定=在途 m1 继续完成，后续消息挂起");
            Thread.sleep(900);

            assertThat(service.committedOffsets("g-flow").get(tp0))
                    .as("暂停期间在途 m1 可确认，但 m2 不得投递")
                    .isEqualTo(2);
            assertThat(offsetsP0).containsExactly(0L, 1L);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-flow").get(tp1)).isEqualTo(6));
            log.info("[断言] partition=flow#1 committedOffset=6 序列={} 判定=其他分区不受暂停影响照常并发",
                    offsetsP1);
            assertThat(offsetsP1).containsExactly(0L, 1L, 2L, 3L, 4L, 5L);

            // 反复暂停结果稳定
            service.pause("g-flow", tp0);
            assertThat(service.isPaused("g-flow", tp0)).isTrue();
            Thread.sleep(300);
            assertThat(service.committedOffsets("g-flow").get(tp0)).isEqualTo(2);
            log.info("[断言] partition=flow#0 判定=重复暂停幂等，位点仍停在 2");

            service.resume("g-flow", tp0);
            service.resume("g-flow", tp0);
            assertThat(service.isPaused("g-flow", tp0)).isFalse();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-flow").get(tp0)).isEqualTo(6));
            assertThat(offsetsP0)
                    .as("恢复后从暂停位置继续，严格有序不跳消息")
                    .containsExactly(0L, 1L, 2L, 3L, 4L, 5L);
            log.info("[断言] partition=flow#0 序列={} 判定=恢复后顺序补全，重复恢复幂等", offsetsP0);
        }
    }

    @Test
    void retryWaitIsHeldWhilePausedAndContinuesAfterResume() throws Exception {
        TopicPartition tp = new TopicPartition("repair", 0);
        Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        CountDownLatch firstAttemptDone = new CountDownLatch(1);

        try (MqService service = new MqService(dataDir, 1000)) {
            ConsumerOptions options = ConsumerOptions.defaults()
                    .maxAttempts(10)
                    .initialBackoff(Duration.ofMillis(50));
            service.registerConsumer("g-repair", msg -> {
                AtomicInteger n = attempts.computeIfAbsent(msg.id(), k -> new AtomicInteger());
                int cur = n.incrementAndGet();
                log.info("[尝试] partition=repair#0 offset={} messageId={} attempt={}",
                        msg.offset(), msg.id(), cur);
                if (msg.id().equals("flaky") && cur < 3) {
                    firstAttemptDone.countDown();
                    throw new RetryableException("临时故障，等待数据修复");
                }
                log.info("[成功] partition=repair#0 offset={} messageId={} attempt={} 判定=修复后处理成功",
                        msg.offset(), msg.id(), cur);
            }, options, List.of(tp));

            service.send("repair", 0, "ok", "p0");
            service.send("repair", 0, "flaky", "p1");

            // 等 flaky 首次失败进入退避
            assertThat(firstAttemptDone.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-repair").get(tp)).isEqualTo(1));
            service.pause("g-repair", tp);
            log.info("[暂停] partition=repair#0 committedOffset=1 messageId=flaky 判定=退避中暂停，到期也不得重投");

            int attemptsAtPause = attempts.get("flaky").get();
            Thread.sleep(800); // 远超退避窗口(50ms)，若暂停失效必然已重投多次
            assertThat(service.committedOffsets("g-repair").get(tp))
                    .as("暂停期间退避到期也不得重投，未确认状态保留")
                    .isEqualTo(1);
            assertThat(attempts.get("flaky").get())
                    .as("暂停期间处理器不应再被调用")
                    .isEqualTo(attemptsAtPause);
            log.info("[断言] partition=repair#0 messageId=flaky 暂停时attempt={} 判定=退避重投被挂起",
                    attemptsAtPause);

            service.resume("g-repair", tp);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-repair").get(tp)).isEqualTo(2));
            assertThat(attempts.get("flaky").get())
                    .as("恢复后退避继续计时重投，第三次成功；副作用前的失败尝试不产生确认")
                    .isEqualTo(3);
            log.info("[断言] partition=repair#0 messageId=flaky 最终attempt=3 committedOffset=2 判定=恢复后成功，不丢不重");
        }
    }
}
