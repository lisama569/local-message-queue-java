package com.github.lisama569.localmessagequeuejava.tests;

import com.github.lisama569.localmessagequeuejava.mq.LocalMessageBroker;
import com.github.lisama569.localmessagequeuejava.mq.model.BrokerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重启恢复测试。
 *
 * <p>同一个数据目录、同一个消费者组语义下「关停 → 重建 broker」等价于成员重启后
 * 重新接管分区。覆盖：</p>
 * <ol>
 *   <li>已确认消息重启后不产生第二次副作用；</li>
 *   <li>一直失败的消息在第一次运行中关停（处于待重试态），重启后继续尝试并最终成功，消息不丢；</li>
 *   <li>关停时仍在分区日志里的未确认消息重启后被继续处理；</li>
 *   <li>位点与去重账本跨重启保持。</li>
 * </ol>
 */
class RestartRecoveryTest {

    private BrokerConfig config() {
        // 退避 100ms：第一次运行期间内它会一直处于 RETRY，关停后待重试态已持久化
        return new BrokerConfig(Duration.ofMillis(100), 100, 64,
                Duration.ofSeconds(5), Duration.ofSeconds(2));
    }

    @Test
    void unackedMessagesSurviveRestart_andCommittedAreNotRepeated(@TempDir Path dataDir)
            throws Exception {
        MqTestSupport.ScriptedHandler handler1 = new MqTestSupport.ScriptedHandler();

        // ---- 第一次运行：ok-1 成功；pending-1 一直可重试失败（处于待重试态）；----
        try (LocalMessageBroker broker = new LocalMessageBroker(
                dataDir, "recover", 1, config(), handler1)) {
            broker.start();
            broker.sendToPartition(0, "ok", "ok-1");
            broker.sendToPartition(0, "pending", "pending-1");
            handler1.poison.add("pending-1");
            // pending-1 持续可重试失败，积压不会清零；显式等待 ok-1 副作用完成即可
            waitForSideEffect(handler1, "ok-1", 3_000);
            assertEquals(1, handler1.sideEffectCount("ok-1"));
            assertTrue(handler1.invocationCount("pending-1") >= 1,
                    "pending-1 应至少被尝试过一次");
            // 再等待至少两次重试，确保关停时待重试状态已持久化
            waitForInvocation(handler1, "pending-1", 2, 3_000);
            Thread.sleep(300);
        } // 优雅关停：待重试状态此时已在每次 retry 时持久化

        int pendingCallsBeforeRestart = handler1.invocationCount("pending-1");

        // ---- 第二次运行：新处理器实例（模拟进程重启），pending-1 改为可成功 ----
        MqTestSupport.ScriptedHandler handler2 = new MqTestSupport.ScriptedHandler();
        try (LocalMessageBroker broker = new LocalMessageBroker(
                dataDir, "recover", 1, config(), handler2)) {
            broker.start();
            // 重启前从未成功过的消息，重启后必须被重新投递且最终成功——不丢失
            assertTrue(broker.awaitDrained(5_000),
                    "重启后 pending-1 应被继续处理并清空积压");
            assertEquals(1, handler2.sideEffectCount("pending-1"),
                    "[判定依据] 未确认消息重启后不得丢失，且只产生一次副作用");
            assertEquals(0, handler2.sideEffectCount("ok-1"),
                    "[判定依据] 已确认消息重启后不得重复产生副作用");
            // 重启后再投递一条新消息，验证位点继续单调推进
            broker.sendToPartition(0, "after-restart", "new-1");
            assertTrue(broker.awaitDrained(5_000), "重启后新消息应被处理");
            assertEquals(1, handler2.sideEffectCount("new-1"));
            System.out.println("[判定依据] pending-1 重启前尝试=" + pendingCallsBeforeRestart
                    + " 重启后副作用=" + handler2.sideEffectCount("pending-1")
                    + "；ok-1 重启后副作用=" + handler2.sideEffectCount("ok-1")
                    + "（应为0，完成账本去重）");
        }
    }

    private void waitForSideEffect(MqTestSupport.ScriptedHandler handler, String id, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && handler.sideEffectCount(id) == 0) {
            Thread.sleep(10);
        }
    }

    private void waitForInvocation(MqTestSupport.ScriptedHandler handler, String id, int min,
                                   long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && handler.invocationCount(id) < min) {
            Thread.sleep(10);
        }
    }
}
