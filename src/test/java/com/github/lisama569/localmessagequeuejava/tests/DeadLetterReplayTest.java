package com.github.lisama569.localmessagequeuejava.tests;

import com.github.lisama569.localmessagequeuejava.mq.LocalMessageBroker;
import com.github.lisama569.localmessagequeuejava.mq.model.BrokerConfig;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.FailureKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 死信重放测试。
 *
 * <ol>
 *   <li>重试耗尽进死信的消息，重放后处理成功：副作用恰好 1 次，积压清零，原始载荷被带回复用；</li>
 *   <li>再次对同一 messageId 重放：因完成账本已有记录，被 dedup-skip，<b>不产生第二个副作用</b>；</li>
 *   <li>重放时仍不可处理（再次死信），重放记录终态被持久化，重启后不会二次处理该重放记录。</li>
 * </ol>
 */
class DeadLetterReplayTest {

    private BrokerConfig config() {
        return new BrokerConfig(Duration.ofMillis(1), 2, 64,
                Duration.ofSeconds(5), Duration.ofSeconds(2));
    }

    @Test
    void replayReDeliversOriginalPayload_butNeverBypassesDedup(@TempDir Path dataDir)
            throws Exception {
        MqTestSupport.ScriptedHandler handler = new MqTestSupport.ScriptedHandler();
        handler.poison.add("recoverable"); // 首次运行：必进死信（maxAttempts=2）

        try (LocalMessageBroker broker = new LocalMessageBroker(
                dataDir, "dlq", 1, config(), handler)) {
            broker.start();
            broker.sendToPartition(0, "原始载荷-42", "recoverable");
            assertTrue(broker.awaitDrained(5_000));

            List<DeadLetterRecord> dead = broker.deadLetters();
            assertEquals(1, dead.size());
            assertEquals(FailureKind.RETRIES_EXHAUSTED, dead.get(0).getFailureKind());
            assertEquals("原始载荷-42", dead.get(0).getOriginalMessage().getPayload(),
                    "[判定依据] 死信保留原始载荷");
            assertEquals(0, handler.sideEffectCount("recoverable"));

            // 解除毒消息，重放：同 id、新位点、replay=true，携带原载荷
            handler.poison.remove("recoverable");
            broker.replayDeadLetter("recoverable");
            assertTrue(broker.awaitDrained(5_000), "重放记录应被处理并清空积压");
            assertEquals(1, handler.sideEffectCount("recoverable"),
                    "[判定依据] 重放成功后副作用恰好1次");
            assertEquals(0, broker.backlog(0));

            // 再次重放同一死信：完成账本拦截，不得绕过重复消费约束
            broker.replayDeadLetter("recoverable");
            assertTrue(broker.awaitDrained(5_000));
            assertEquals(1, handler.sideEffectCount("recoverable"),
                    "[判定依据] 已成功消息的再次重放必须被去重，副作用仍为1");

            System.out.println("[判定依据] recoverable 原死信载荷=原始载荷-42 "
                    + "handler总调用=" + handler.invocationCount("recoverable")
                    + " 副作用=" + handler.sideEffectCount("recoverable")
                    + "（第二次重放走 dedup-skip）");
        }
    }

    @Test
    void replayRecordTerminalStateSurvivesRestart(@TempDir Path dataDir) throws Exception {
        // 第一轮：消息进死信；重放时仍为毒消息 => 重放记录再次死信，终态持久化
        MqTestSupport.ScriptedHandler h1 = new MqTestSupport.ScriptedHandler();
        h1.poison.add("stuck");
        try (LocalMessageBroker broker = new LocalMessageBroker(
                dataDir, "dlq2", 1, config(), h1)) {
            broker.start();
            broker.sendToPartition(0, "stuck-payload", "stuck");
            assertTrue(broker.awaitDrained(5_000));
            assertEquals(1, broker.deadLetters().size());
            broker.replayDeadLetter("stuck");
            assertTrue(broker.awaitDrained(5_000), "重放记录再次死信后积压清零");
            int callsBefore = h1.invocationCount("stuck");
            assertTrue(callsBefore >= 3, "原始2次 + 重放至少1次，实际=" + callsBefore);
        }

        // 第二轮：重启后即使解除毒消息，那条「已再次死信」的重放记录也不应被二次处理
        MqTestSupport.ScriptedHandler h2 = new MqTestSupport.ScriptedHandler();
        try (LocalMessageBroker broker = new LocalMessageBroker(
                dataDir, "dlq2", 1, config(), h2)) {
            broker.start();
            assertTrue(broker.awaitDrained(2_000));
            assertEquals(0, h2.sideEffectCount("stuck"),
                    "[判定依据] 已终态的重放记录重启后不得自动再处理");
            assertEquals(0, h2.invocationCount("stuck"),
                    "重启后不应再调用handler处理该重放记录");
            assertEquals(0, broker.backlog(0), "积压应为0");
            System.out.println("[判定依据] stuck 重启后 handler调用=" + h2.invocationCount("stuck")
                    + " 副作用=" + h2.sideEffectCount("stuck") + "（重放终态已持久化）");
        }
    }
}
