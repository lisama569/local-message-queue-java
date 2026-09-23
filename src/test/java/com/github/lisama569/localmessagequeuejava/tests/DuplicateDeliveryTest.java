package com.github.lisama569.localmessagequeuejava.tests;

import com.github.lisama569.localmessagequeuejava.mq.LocalMessageBroker;
import com.github.lisama569.localmessagequeuejava.mq.model.BrokerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 重复投递测试。
 *
 * <p>场景：消息 id=dup-1 前两次处理声明可重试失败（模拟 at-least-once 重投/抖动），
 * 随后成功；生产端还把同一 messageId 再次作为新记录投递（重复投递）。
 * 判定依据：完成账本按 messageId 去重——处理器可被调用多次，
 * 但副作用计数器必须且只能为 1。</p>
 */
class DuplicateDeliveryTest {

    @Test
    void duplicateDeliveryProducesSingleSideEffect(@TempDir Path dataDir) throws Exception {
        BrokerConfig config = new BrokerConfig(Duration.ofMillis(5), 5, 64,
                Duration.ofSeconds(5), Duration.ofSeconds(2));
        MqTestSupport.ScriptedHandler handler = new MqTestSupport.ScriptedHandler();
        handler.flakyFailUntil.put("dup-1", 2); // 前2次重试，第3次成功

        try (LocalMessageBroker broker = new LocalMessageBroker(
                dataDir, "dup-topic", 1, config, handler)) {
            broker.start();
            broker.sendToPartition(0, "payload-v1", "dup-1");
            // 等首条成功后再制造「重复投递」：同 id 新记录（不同位点）
            assertTrue(broker.awaitDrained(5_000), "first delivery should drain");
            assertEquals(1, handler.sideEffectCount("dup-1"),
                    "首次成功后副作用应为1");

            broker.sendToPartition(0, "payload-v1-duplicate", "dup-1");
            assertTrue(broker.awaitDrained(5_000), "duplicate delivery should drain");

            assertEquals(1, handler.sideEffectCount("dup-1"),
                    "[判定依据] 重复投递不得产生重复副作用");
            // 重复记录仍被正常确认推进（位点不丢、不阻塞）
            assertEquals(0, broker.backlog(0), "重复记录应被确认，积压归零");
            System.out.println("[判定依据] messageId=dup-1 handler调用="
                    + handler.invocationCount("dup-1")
                    + " 副作用=" + handler.sideEffectCount("dup-1")
                    + "（>=1 次调用但仅1次副作用，dedup-skip 见日志 offset=1）");
        }
    }
}
