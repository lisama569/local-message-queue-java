package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.consumer.ConsumerOptions;
import com.github.lisama569.localmessagequeuejava.mq.consumer.MessageConsumer;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 并发消费：同一分区内按位点顺序处理，不同分区并发推进。
 */
class ConcurrentConsumptionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentConsumptionTest.class);

    @TempDir
    Path dataDir;

    @Test
    void samePartitionOrderedAndPartitionsConcurrent() throws Exception {
        String topic = "orders";
        int partitions = 3;
        int perPartition = 10;
        List<TopicPartition> tps = IntStream.range(0, partitions)
                .mapToObj(p -> new TopicPartition(topic, p)).toList();

        try (MqService service = new MqService(dataDir, 1000)) {
            for (int p = 0; p < partitions; p++) {
                for (int i = 0; i < perPartition; i++) {
                    Message msg = service.send(topic, p, "m-" + p + "-" + i, "payload-" + p + "-" + i);
                    log.info("[发送] partition={} offset={} messageId={}", msg.topic() + "#" + msg.partition(),
                            msg.offset(), msg.id());
                }
            }

            // 首条消息在 3 个分区上汇合于屏障：若分区间不能并发，屏障将超时
            CyclicBarrier barrier = new CyclicBarrier(partitions);
            Map<Integer, List<Long>> offsetsByPartition = new ConcurrentHashMap<>();
            CountDownLatch done = new CountDownLatch(partitions * perPartition);
            Map<String, String> threadByPartition = new ConcurrentHashMap<>();

            MessageConsumer consumer = service.registerConsumer("g1", msg -> {
                if (msg.offset() == 0) {
                    barrier.await(5, TimeUnit.SECONDS);
                }
                offsetsByPartition.computeIfAbsent(msg.partition(), k -> new CopyOnWriteArrayList<>())
                        .add(msg.offset());
                threadByPartition.putIfAbsent(msg.topic() + "#" + msg.partition(),
                        Thread.currentThread().getName());
                log.info("[消费] partition={} offset={} messageId={} 判定=按序处理",
                        msg.topic() + "#" + msg.partition(), msg.offset(), msg.id());
                done.countDown();
            }, ConsumerOptions.defaults(), tps);

            assertThat(done.await(30, TimeUnit.SECONDS))
                    .as("全部消息应在超时前被并发消费完成（屏障已验证跨分区并发）")
                    .isTrue();

            for (int p = 0; p < partitions; p++) {
                List<Long> offsets = offsetsByPartition.get(p);
                assertThat(offsets).as("分区 %d 内位点必须严格递增", p)
                        .containsExactlyElementsOf(IntStream.range(0, perPartition)
                                .mapToLong(i -> i).boxed().toList());
                log.info("[断言] partition={} 位点序列={} 判定=分区内严格有序", topic + "#" + p, offsets);
            }
            assertThat(threadByPartition).hasSize(partitions);
            log.info("[断言] 各分区处理线程={} 判定=不同分区由不同 worker 并发推进", threadByPartition);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    tps.forEach(tp -> assertThat(service.committedOffsets("g1").get(tp))
                            .as("分区 %s 确认位点", tp.key()).isEqualTo(perPartition)));
            consumer.close();
        }
    }
}
