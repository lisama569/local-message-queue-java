package com.github.lisama569.localmessagequeuejava.tests;

import com.github.lisama569.localmessagequeuejava.mq.LocalMessageBroker;
import com.github.lisama569.localmessagequeuejava.mq.model.BrokerConfig;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.FailureKind;
import com.github.lisama569.localmessagequeuejava.mq.model.HandleResult;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.handler.MessageHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败分类 / 退避重试 / 死信测试。
 *
 * <p>三类失败必须可区分：</p>
 * <ul>
 *   <li>{@code exhaust}：可重试失败持续到最大尝试次数 → 死信，kind=RETRIES_EXHAUSTED；</li>
 *   <li>{@code fatal}：首次即声明不可重试 → 立刻死信，kind=NON_RETRYABLE，attempts=1；</li>
 *   <li>{@code boom}：处理器直接抛异常 → 默认按可重试失败处理，耗尽后同样进死信。</li>
 * </ul>
 */
class RetryAndDeadLetterTest {

    @Test
    void failureKindsAreDistinguishable_andDeadLetterKeepsPayload(@TempDir Path dataDir)
            throws Exception {
        BrokerConfig config = new BrokerConfig(Duration.ofMillis(1), 3, 64,
                Duration.ofSeconds(5), Duration.ofSeconds(2));
        AtomicInteger exhaustCalls = new AtomicInteger();
        AtomicInteger boomCalls = new AtomicInteger();

        MessageHandler handler = (Message m) -> switch (m.getId()) {
            case "exhaust" -> {
                exhaustCalls.incrementAndGet();
                yield HandleResult.retry("always transient");
            }
            case "fatal" -> HandleResult.deadLetter("explicit non-retryable reason X");
            case "boom" -> {
                boomCalls.incrementAndGet();
                throw new IllegalStateException("kaboom");
            }
            default -> HandleResult.ack();
        };

        try (LocalMessageBroker broker = new LocalMessageBroker(
                dataDir, "retry", 1, config, handler)) {
            broker.start();
            broker.sendToPartition(0, "payload-exhaust", "exhaust");
            broker.sendToPartition(0, "payload-fatal", "fatal");
            broker.sendToPartition(0, "payload-boom", "boom");
            broker.sendToPartition(0, "payload-ok", "ok-1");

            assertTrue(broker.awaitDrained(10_000), "全部消息（含死信）应到达终态");

            List<DeadLetterRecord> dead = broker.deadLetters();
            assertEquals(3, dead.size(), "应有3条死信（2个耗尽 + 1个不可重试）");

            DeadLetterRecord exhaust = find(dead, "exhaust");
            assertEquals(FailureKind.RETRIES_EXHAUSTED, exhaust.getFailureKind(),
                    "[判定依据] 可重试耗尽必须标 RETRIES_EXHAUSTED");
            assertEquals(3, exhaust.getAttempts(), "最多尝试3次");
            assertEquals("payload-exhaust", exhaust.getOriginalMessage().getPayload(),
                    "[判定依据] 死信必须保留原始载荷");
            assertNotNull(exhaust.getReason());
            assertEquals(0, exhaust.getOriginalMessage().getOffset());

            DeadLetterRecord fatal = find(dead, "fatal");
            assertEquals(FailureKind.NON_RETRYABLE, fatal.getFailureKind(),
                    "[判定依据] 显式不可重试必须标 NON_RETRYABLE");
            assertEquals(1, fatal.getAttempts(), "不可重试不应重试，attempts=1");
            assertEquals("explicit non-retryable reason X", fatal.getReason());
            assertEquals("payload-fatal", fatal.getOriginalMessage().getPayload());

            DeadLetterRecord boom = find(dead, "boom");
            assertEquals(FailureKind.RETRIES_EXHAUSTED, boom.getFailureKind(),
                    "[判定依据] 抛出异常默认可重试，耗尽后进死信");
            assertEquals(3, boom.getAttempts());
            assertTrue(boom.getReason().contains("IllegalStateException"),
                    "失败原因应保留异常类型: " + boom.getReason());

            // 后续正常消息不受前序死信阻塞：严格有序但终态后继续推进
            System.out.println("[判定依据] 死信数=" + dead.size()
                    + " exhaust.kind=" + exhaust.getFailureKind() + "/attempts=" + exhaust.getAttempts()
                    + " fatal.kind=" + fatal.getFailureKind() + "/attempts=" + fatal.getAttempts()
                    + " boom.kind=" + boom.getFailureKind() + "/attempts=" + boom.getAttempts()
                    + " => 三类失败可区分，原始载荷保留");
        }
    }

    private DeadLetterRecord find(List<DeadLetterRecord> records, String id) {
        return records.stream()
                .filter(r -> r.getOriginalMessage().getId().equals(id))
                .findFirst()
                .orElseThrow(() -> new AssertionError("dead letter not found: " + id
                        + ", actual=" + records.stream().map(r -> r.getOriginalMessage().getId()).toList()));
    }
}
