package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.consumer.ConsumerOptions;
import com.github.lisama569.localmessagequeuejava.mq.exception.NonRetryableException;
import com.github.lisama569.localmessagequeuejava.mq.exception.RetryableException;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.FailureType;
import com.github.lisama569.localmessagequeuejava.mq.model.ReplayOutcome;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 死信与重放：可重试/不可重试/重试耗尽三类失败可区分；
 * 死信保留原始载荷与失败原因；重放受去重约束不产生重复副作用。
 */
class DeadLetterReplayTest {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterReplayTest.class);

    @TempDir
    Path dataDir;

    @Test
    void failureClassificationAndReplay() {
        TopicPartition tp = new TopicPartition("jobs", 0);
        Map<String, Integer> sideEffectCount = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        // 重放后 x-bad 恢复正常
        Map<String, Boolean> healed = new ConcurrentHashMap<>();

        try (MqService service = new MqService(dataDir, 100)) {
            ConsumerOptions options = ConsumerOptions.defaults()
                    .maxAttempts(3).initialBackoff(Duration.ofMillis(50));
            service.registerConsumer("g-dlq", msg -> {
                attempts.computeIfAbsent(msg.id(), k -> new AtomicInteger()).incrementAndGet();
                switch (msg.id()) {
                    case "r-ok" -> {
                        if (attempts.get(msg.id()).get() < 3) {
                            throw new RetryableException("临时故障");
                        }
                        sideEffectCount.merge(msg.id(), 1, Integer::sum);
                    }
                    case "n-bad" -> throw new NonRetryableException("数据格式非法");
                    case "x-bad" -> {
                        if (!healed.getOrDefault(msg.id(), false)) {
                            throw new RetryableException("下游持续不可用");
                        }
                        sideEffectCount.merge(msg.id(), 1, Integer::sum);
                    }
                    default -> sideEffectCount.merge(msg.id(), 1, Integer::sum);
                }
            }, options, List.of(tp));

            service.send("jobs", 0, "r-ok", "payload-r");
            service.send("jobs", 0, "n-bad", "payload-n");
            service.send("jobs", 0, "x-bad", "payload-x");

            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(sideEffectCount.get("r-ok")).isEqualTo(1);
                assertThat(service.deadLetters("g-dlq")).hasSize(2);
            });

            Map<String, DeadLetterRecord> dlq = new ConcurrentHashMap<>();
            service.deadLetters("g-dlq").forEach(r -> dlq.put(r.messageId(), r));
            DeadLetterRecord nBad = dlq.get("n-bad");
            DeadLetterRecord xBad = dlq.get("x-bad");
            log.info("[断言] messageId=n-bad failureType={} attempts={} 判定=不可重试立即死信",
                    nBad.failureType(), nBad.attempts());
            log.info("[断言] messageId=x-bad failureType={} attempts={} 判定=重试耗尽死信",
                    xBad.failureType(), xBad.attempts());
            assertThat(nBad.failureType()).isEqualTo(FailureType.NON_RETRYABLE);
            assertThat(nBad.attempts()).isEqualTo(1);
            assertThat(xBad.failureType()).isEqualTo(FailureType.RETRY_EXHAUSTED);
            assertThat(xBad.attempts()).isEqualTo(3);
            assertThat(nBad.payload()).isEqualTo("payload-n");
            assertThat(xBad.payload()).isEqualTo("payload-x");
            assertThat(nBad.errorMessage()).contains("数据格式非法");
            assertThat(xBad.errorMessage()).contains("下游持续不可用");
            assertThat(nBad.failureType()).isNotEqualTo(xBad.failureType());

            // 重放 x-bad：修复后成功消费
            healed.put("x-bad", true);
            ReplayOutcome first = service.replayDeadLetter("g-dlq", "x-bad");
            log.info("[重放] messageId=x-bad newOffset={} deduplicated={} 判定=首次重放",
                    first.offset(), first.deduplicated());
            assertThat(first.deduplicated()).isFalse();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(sideEffectCount.get("x-bad")).isEqualTo(1));

            // 再次重放同一死信：消息已确认，必须被去重，不产生重复副作用
            ReplayOutcome second = service.replayDeadLetter("g-dlq", "x-bad");
            log.info("[重放] messageId=x-bad newOffset={} deduplicated={} 判定=重复重放被去重约束拦截",
                    second.offset(), second.deduplicated());
            assertThat(second.deduplicated()).isTrue();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-dlq").get(tp)).isEqualTo(5));
            assertThat(sideEffectCount.get("x-bad"))
                    .as("重放不得绕过重复消费约束，x-bad 副作用仍只执行 1 次").isEqualTo(1);

            // 重放 n-bad：仍不可重试失败，重新进入死信，原因可区分
            service.replayDeadLetter("g-dlq", "n-bad");
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                List<DeadLetterRecord> all = service.deadLetters("g-dlq");
                assertThat(all.stream().filter(r -> r.messageId().equals("n-bad"))).hasSize(2);
                assertThat(service.committedOffsets("g-dlq").get(tp)).isEqualTo(6);
            });
            log.info("[断言] messageId=n-bad 死信次数=2 判定=重放后仍按不可重试失败再次死信");
        }
    }
}
