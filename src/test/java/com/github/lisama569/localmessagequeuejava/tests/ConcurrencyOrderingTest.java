package com.github.lisama569.localmessagequeuejava.tests;

import com.github.lisama569.localmessagequeuejava.mq.LocalMessageBroker;
import com.github.lisama569.localmessagequeuejava.mq.model.BrokerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发消费与分区有序测试。
 *
 * <p>判定依据：两个分区各自的首条消息处理耗时 300ms。若分区间串行，总时长 ≥ 600ms；
 * 若分区间并发推进，总时长约 300ms；同时每个分区内副作用必须严格按位点出现。</p>
 */
class ConcurrencyOrderingTest {

    @Test
    void partitionsProcessConcurrently_butInOrderWithinPartition(@TempDir Path dataDir)
            throws Exception {
        BrokerConfig config = new BrokerConfig(Duration.ofMillis(10), 3, 64,
                Duration.ofSeconds(5), Duration.ofSeconds(5));
        MqTestSupport.ScriptedHandler handler = new MqTestSupport.ScriptedHandler();
        // 关键延迟在首条消息被处理前就配好
        handler.delays.put("p0-0", 300L);
        handler.delays.put("p1-0", 300L);
        handler.defaultDelayMs = 20L;

        try (LocalMessageBroker broker = new LocalMessageBroker(
                dataDir, "orders", 2, config, handler)) {
            broker.start();
            broker.sendToPartition(0, "p0-0", "p0-0");
            broker.sendToPartition(1, "p1-0", "p1-0");
            broker.sendToPartition(0, "p0-1", "p0-1");
            broker.sendToPartition(0, "p0-2", "p0-2");
            broker.sendToPartition(1, "p1-1", "p1-1");
            broker.sendToPartition(1, "p1-2", "p1-2");

            long start = System.currentTimeMillis();
            assertTrue(broker.awaitDrained(10_000), "all partitions should drain");
            long elapsed = System.currentTimeMillis() - start;

            for (String id : List.of("p0-0", "p0-1", "p0-2", "p1-0", "p1-1", "p1-2")) {
                assertEquals(1, handler.sideEffectCount(id),
                        "[判定依据] messageId=" + id + " 必须恰好产生一次副作用（不丢失/不重复）");
            }
            List<String> order = handler.snapshotOrder();
            assertTrue(order.indexOf("p0-0") < order.indexOf("p0-1"),
                    "partition=0 offset 顺序被破坏: " + order);
            assertTrue(order.indexOf("p0-1") < order.indexOf("p0-2"),
                    "partition=0 offset 顺序被破坏: " + order);
            assertTrue(order.indexOf("p1-0") < order.indexOf("p1-1"),
                    "partition=1 offset 顺序被破坏: " + order);
            assertTrue(order.indexOf("p1-1") < order.indexOf("p1-2"),
                    "partition=1 offset 顺序被破坏: " + order);
            // 并行硬判据：两组 300ms 重叠完成（串行下限 600ms，留 500ms 余量防 CI 抖动）
            assertTrue(elapsed < 500,
                    "[判定依据] 分区间应并发：两组300ms处理总耗时=" + elapsed + "ms，若串行应≥600ms");
            System.out.println("[判定依据] 分区并发 drained elapsedMs=" + elapsed
                    + "（两个分区首条各300ms）全局副作用顺序=" + order
                    + " => 分区内有序、分区间并发");
        }
    }
}
