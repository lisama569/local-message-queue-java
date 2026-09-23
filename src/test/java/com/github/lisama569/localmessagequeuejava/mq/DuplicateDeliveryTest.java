package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.consumer.ConsumerOptions;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 重复投递：同一消息标识被重复投递时，副作用只发生一次。
 */
class DuplicateDeliveryTest {

    private static final Logger log = LoggerFactory.getLogger(DuplicateDeliveryTest.class);

    @TempDir
    Path dataDir;

    @Test
    void duplicateDeliveryProducesNoDuplicateSideEffect() throws Exception {
        TopicPartition tp = new TopicPartition("payments", 0);
        Map<String, Integer> sideEffectCount = new ConcurrentHashMap<>();
        CountDownLatch firstConsumed = new CountDownLatch(1);

        try (MqService service = new MqService(dataDir, 100)) {
            service.registerConsumer("g-dup", msg -> {
                sideEffectCount.merge(msg.id(), 1, Integer::sum);
                log.info("[副作用] partition={} offset={} messageId={} 判定=业务副作用执行",
                        msg.topic() + "#" + msg.partition(), msg.offset(), msg.id());
                firstConsumed.countDown();
            }, ConsumerOptions.defaults(), List.of(tp));

            Message first = service.send("payments", 0, "pay-1", "amount=100");
            log.info("[发送] partition=payments#0 offset={} messageId={}", first.offset(), first.id());
            assertThat(firstConsumed.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-dup").get(tp)).isEqualTo(1));

            // 模拟重复投递：同一消息标识再次入队（生产端重试/网络重发）
            Message dup = service.send("payments", 0, "pay-1", "amount=100");
            log.info("[重投] partition=payments#0 offset={} messageId={} 判定=模拟重复投递", dup.offset(), dup.id());

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-dup").get(tp)).isEqualTo(2));

            assertThat(sideEffectCount.get("pay-1"))
                    .as("重复投递不得产生重复副作用，messageId=pay-1 应只执行 1 次")
                    .isEqualTo(1);
            log.info("[断言] partition=payments#0 messageId=pay-1 副作用次数={} 判定=重复投递被去重",
                    sideEffectCount.get("pay-1"));
        }
    }
}
