package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.consumer.ConsumerOptions;
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
 * 重启恢复：处理超时/进程中断后，未确认消息不丢失，已确认消息不重复产生副作用。
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    @TempDir
    Path dataDir;

    @Test
    void unackedSurviveRestartAndAckedNotReplayed() throws Exception {
        TopicPartition tp = new TopicPartition("tasks", 0);
        Map<String, Integer> sideEffectCount = new ConcurrentHashMap<>();

        // 阶段一：m0/m1 正常确认；m2 处理挂起触发超时，按可重试失败落盘后“进程退出”
        try (MqService service1 = new MqService(dataDir, 100)) {
            for (int i = 0; i < 5; i++) {
                var msg = service1.send("tasks", 0, "m" + i, "job-" + i);
                log.info("[发送] partition=tasks#0 offset={} messageId={}", msg.offset(), msg.id());
            }
            CountDownLatch twoDone = new CountDownLatch(2);
            ConsumerOptions options = ConsumerOptions.defaults()
                    .processingTimeout(Duration.ofMillis(500))
                    .initialBackoff(Duration.ofMillis(50));
            service1.registerConsumer("g-restart", msg -> {
                if (msg.id().equals("m2")) {
                    log.info("[挂起] partition=tasks#0 offset={} messageId={} 判定=模拟处理超时",
                            msg.offset(), msg.id());
                    try {
                        Thread.sleep(10_000);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    return; // 副作用未执行即被中断
                }
                sideEffectCount.merge(msg.id(), 1, Integer::sum);
                log.info("[副作用] partition=tasks#0 offset={} messageId={} 判定=阶段一确认",
                        msg.offset(), msg.id());
                twoDone.countDown();
            }, options, List.of(tp));

            assertThat(twoDone.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service1.committedOffsets("g-restart").get(tp)).isEqualTo(2));
            // 等待 m2 超时被分类为可重试并落盘
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service1.deadLetters("g-restart")).isEmpty());
            log.info("[阶段一] partition=tasks#0 确认位点=2 messageId=m2 判定=超时→可重试退避，等待重启");
        }

        // 阶段二：同一数据目录重启，未确认消息必须恢复投递，已确认消息不得重复执行
        try (MqService service2 = new MqService(dataDir, 100)) {
            CountDownLatch allDone = new CountDownLatch(3);
            service2.registerConsumer("g-restart", msg -> {
                sideEffectCount.merge(msg.id(), 1, Integer::sum);
                log.info("[副作用] partition=tasks#0 offset={} messageId={} 判定=阶段二恢复消费",
                        msg.offset(), msg.id());
                allDone.countDown();
            }, ConsumerOptions.defaults(), List.of(tp));

            assertThat(allDone.await(15, TimeUnit.SECONDS))
                    .as("重启后 m2/m3/m4 必须被恢复消费（未确认不丢失）").isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service2.committedOffsets("g-restart").get(tp)).isEqualTo(5));

            sideEffectCount.forEach((id, count) ->
                    log.info("[断言] partition=tasks#0 messageId={} 副作用次数={} 判定={}", id, count,
                            count == 1 ? "恰好一次" : "异常重复"));
            assertThat(sideEffectCount)
                    .as("已确认消息不得重复执行，未确认消息不得丢失：5 条消息各执行恰好 1 次")
                    .containsOnly(Map.entry("m0", 1), Map.entry("m1", 1), Map.entry("m2", 1),
                            Map.entry("m3", 1), Map.entry("m4", 1));
        }
    }
}
