package com.github.lisama569.localmessagequeuejava.tests;

import com.github.lisama569.localmessagequeuejava.mq.LocalMessageBroker;
import com.github.lisama569.localmessagequeuejava.mq.exception.BacklogCapacityExceededException;
import com.github.lisama569.localmessagequeuejava.mq.exception.BrokerShutdownException;
import com.github.lisama569.localmessagequeuejava.mq.model.BrokerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 积压控制与优雅关停测试。
 *
 * <p>判定依据：</p>
 * <ul>
 *   <li>未确认积压达到 {@code partitionPrefetch} → 抛
 *       {@link BacklogCapacityExceededException}（容量拒绝）；</li>
 *   <li>关停开始后生产 → 抛 {@link BrokerShutdownException}（关停拒绝）；
 *       两种拒绝是不同类型，调用方可明确区分；</li>
 *   <li>关停期间在途消息跑完并确认落盘，同目录重启后积压为 0、不重复处理。</li>
 * </ul>
 */
class CapacityAndShutdownTest {

    @Test
    void capacityRejection_andShutdownRejection_areDistinct(@TempDir Path dataDir) throws Exception {
        int capacity = 3;
        BrokerConfig config = new BrokerConfig(Duration.ofMillis(10), 5, capacity,
                Duration.ofSeconds(10), Duration.ofSeconds(10));
        MqTestSupport.ScriptedHandler handler = new MqTestSupport.ScriptedHandler();
        // 第一条消息进入处理器后阻塞很久：它在途时，3 条已落盘未确认 => 积压=容量
        java.util.concurrent.CountDownLatch firstInHandler = new java.util.concurrent.CountDownLatch(1);
        handler.gate = firstInHandler;
        handler.delays.put("block-0", 2_000L);

        LocalMessageBroker broker = new LocalMessageBroker(
                dataDir, "cap", 1, config, handler);
        broker.start();
        broker.sendToPartition(0, "b0", "block-0");
        assertTrue(firstInHandler.await(2, java.util.concurrent.TimeUnit.SECONDS),
                "首条消息应已进入处理器并在途阻塞");
        // 此时 block-0 正在在途处理。再补两条排队记录，积压恰好达到容量
        broker.sendToPartition(0, "b1", "block-1");
        broker.sendToPartition(0, "b2", "block-2");

        assertEquals(capacity, broker.backlog(0), "3条落盘未确认 => 积压达到容量边界");

        // 第4条被容量边界拒绝（与关停拒绝类型不同）
        BacklogCapacityExceededException capEx = assertThrows(
                BacklogCapacityExceededException.class,
                () -> broker.sendToPartition(0, "overflow", "overflow-1"),
                "[判定依据] 达到容量边界必须明确拒绝（BacklogCapacityExceededException）");
        assertTrue(capEx.getMessage().contains("capacity 3"), capEx.getMessage());
        System.out.println("[判定依据] 容量拒绝: " + capEx.getMessage());

        // 优雅关停：在途 block-0 跑完，排队两条继续处理完
        broker.shutdown();

        // 关停门关闭后新生产被另一种异常拒绝
        BrokerShutdownException shutEx = assertThrows(
                BrokerShutdownException.class,
                () -> broker.sendToPartition(0, "after-stop", "after-stop-1"),
                "[判定依据] 关停期间必须以独立异常类型拒绝新任务");
        System.out.println("[判定依据] 关停拒绝: " + shutEx.getMessage());

        // 关停期间三条消息均确认落盘：重启后积压为0、无重复副作用
        MqTestSupport.ScriptedHandler handler2 = new MqTestSupport.ScriptedHandler();
        try (LocalMessageBroker restarted = new LocalMessageBroker(
                dataDir, "cap", 1, config, handler2)) {
            restarted.start();
            assertTrue(restarted.awaitDrained(5_000));
            assertEquals(0, restarted.backlog(0), "[判定依据] 关停前在途/排队消息状态已持久化");
            assertEquals(0, handler2.sideEffectCount("block-0"),
                    "已在关停中确认的消息重启后不重复处理");
            for (String id : new String[]{"block-0", "block-1", "block-2"}) {
                assertEquals(1, handler.sideEffectCount(id),
                        "在途/排队消息在关停期间恰好处理一次: " + id);
            }
            System.out.println("[判定依据] 重启后 backlog=0，block-0/1/2 关停中各副作用1次");
        }
    }
}
