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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * 订阅者不停机热替换：切换窗口内新老订阅者不重不漏地处理同一组分区。
 * 验证点：
 * <ul>
 *   <li>旧订阅者先优雅关停（等在途完成落盘），切换期间同一分区不会被新老同时处理；</li>
 *   <li>在途被中断的消息不丢失（重启恢复路径），已确认消息不重复产生副作用；</li>
 *   <li>切换后从正确的未确认位置继续，全部消息恰好处理一次；</li>
 *   <li>暂停状态跨热替换保留；同组重复注册被拒绝。</li>
 * </ul>
 */
class HotSwapTest {

    private static final Logger log = LoggerFactory.getLogger(HotSwapTest.class);

    @TempDir
    Path dataDir;

    @Test
    void hotSwapIsExclusiveAndNoMessageLostOrDuplicated() throws Exception {
        TopicPartition tp = new TopicPartition("deploy", 0);
        int total = 20;
        Map<String, Integer> sideEffectCount = new ConcurrentHashMap<>();
        Map<String, String> handlerOfId = new ConcurrentHashMap<>();
        List<String> handledInOrder = new CopyOnWriteArrayList<>();
        // 旧订阅者处理 m0..m9 后，在 m10 上挂起（模拟切换瞬间的在途处理）
        CountDownLatch reachedBlocked = new CountDownLatch(1);
        CountDownLatch releaseBlocked = new CountDownLatch(1);

        try (MqService service = new MqService(dataDir, 1000)) {
            for (int i = 0; i < total; i++) {
                Message msg = service.send("deploy", 0, "m" + i, "job-" + i);
                log.info("[发送] partition=deploy#0 offset={} messageId={}", msg.offset(), msg.id());
            }

            ConsumerOptions options = ConsumerOptions.defaults()
                    .processingTimeout(Duration.ofMillis(600))
                    .initialBackoff(Duration.ofMillis(50));
            service.registerConsumer("g-deploy", msg -> {
                if (msg.offset() == 10) {
                    log.info("[挂起] partition=deploy#0 offset={} messageId={} 判定=旧订阅者在途，热切换将中断它",
                            msg.offset(), msg.id());
                    reachedBlocked.countDown();
                    try {
                        releaseBlocked.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return; // 不产生副作用
                }
                sideEffectCount.merge(msg.id(), 1, Integer::sum);
                handlerOfId.put(msg.id(), "old");
                handledInOrder.add(msg.id());
                log.info("[旧处理] partition=deploy#0 offset={} messageId={} 判定=旧订阅者确认",
                        msg.offset(), msg.id());
            }, options, List.of(tp));

            // 等旧订阅者确认 m0..m9 且 m10 在途挂起
            assertThat(reachedBlocked.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-deploy").get(tp)).isEqualTo(10));
            log.info("[切换前] partition=deploy#0 committedOffset=10 headOffset=10 判定=10条已确认，m10在途");

            // replaceConsumer 是阻塞调用：必须等旧订阅者优雅关停（在途超时落盘）后才会启动新订阅者。
            // 若旧订阅者未完成关停就返回（新老可能并存），耗时会远小于处理超时宽限；此处验证"先关旧再启新"。
            long t0 = System.currentTimeMillis();
            var newConsumer = service.replaceConsumer("g-deploy", msg -> {
                log.info("[新处理] partition=deploy#0 offset={} messageId={} 判定=新订阅者处理",
                        msg.offset(), msg.id());
                sideEffectCount.merge(msg.id(), 1, Integer::sum);
                handlerOfId.putIfAbsent(msg.id(), "new");
                handledInOrder.add(msg.id());
            }, options, List.of(tp));
            long elapsed = System.currentTimeMillis() - t0;
            assertThat(elapsed)
                    .as("切换耗时 %dms 应覆盖旧订阅者在途处理超时关停（>=600ms），证明先关旧再启新", elapsed)
                    .isGreaterThanOrEqualTo(500);
            log.info("[切换完成] partition=deploy#0 耗时={}ms 判定=旧订阅者在途超时落盘后才启动新订阅者，切换窗口无重叠",
                    elapsed);

            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-deploy").get(tp)).isEqualTo(total));

            for (int i = 0; i < total; i++) {
                String id = "m" + i;
                assertThat(sideEffectCount.get(id))
                        .as("热替换不重不漏：messageId=%s 必须恰好处理 1 次", id)
                        .isEqualTo(1);
            }
            assertThat(handledInOrder)
                    .as("切换后从未确认位置 m10 继续，位点严格有序不跳消息")
                    .containsExactlyElementsOf(java.util.stream.IntStream.range(0, total)
                            .mapToObj(i -> "m" + i).toList());
            log.info("[断言] partition=deploy#0 处理序列={} 判定=20条按序各处理一次", handledInOrder);
            log.info("[断言] partition=deploy#0 新旧分界: m0..m9={}, m10..m19 新订阅者接管",
                    handlerOfId.get("m9"));
            assertThat(handlerOfId.get("m9")).isEqualTo("old");
            assertThat(handlerOfId.get("m10")).isEqualTo("new");
            assertThat(handlerOfId.get("m19")).isEqualTo("new");

            // 同组重复注册必须被拒绝，防止新老订阅者并存（热替换必须走 replaceConsumer）
            assertThatThrownBy(() -> service.registerConsumer("g-deploy",
                    msg -> {
                    }, ConsumerOptions.defaults(), List.of(tp)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("replaceConsumer");
            log.info("[断言] group=g-deploy 判定=重复注册被拒绝，热替换只能走 replaceConsumer");

            newConsumer.close();
        }
    }

    @Test
    void pausedStateSurvivesHotSwap() throws Exception {
        TopicPartition tp0 = new TopicPartition("canary", 0);
        TopicPartition tp1 = new TopicPartition("canary", 1);
        CountDownLatch firstDone = new CountDownLatch(1);

        try (MqService service = new MqService(dataDir, 1000)) {
            service.registerConsumer("g-canary", msg -> firstDone.countDown(),
                    ConsumerOptions.defaults(), List.of(tp0, tp1));
            service.send("canary", 0, "a0", "p0");
            assertThat(firstDone.await(10, TimeUnit.SECONDS)).isTrue();

            // 暂停 0 号分区，发新消息；热替换后 0 号分区必须仍暂停、1 号分区照常
            service.pause("g-canary", tp0);
            service.send("canary", 0, "a1", "p1");
            service.send("canary", 1, "b0", "q0");

            CountDownLatch p1Done = new CountDownLatch(1);
            Map<String, Integer> counts = new ConcurrentHashMap<>();
            service.replaceConsumer("g-canary", msg -> {
                counts.merge(msg.id(), 1, Integer::sum);
                if (msg.partition() == 1) {
                    p1Done.countDown();
                }
            }, ConsumerOptions.defaults(), List.of(tp0, tp1));

            assertThat(p1Done.await(10, TimeUnit.SECONDS)).isTrue();
            // 留足调度窗口，确认暂停的 0 号分区没有被投递
            Thread.sleep(500);
            assertThat(service.isPaused("g-canary", tp0)).isTrue();
            assertThat(counts).doesNotContainKey("a1");
            log.info("[断言] partition=canary#0 messageId=a1 判定=暂停状态跨热替换保留，未投递");

            // 恢复后 a1 恰好处理一次，且不跳过
            service.resume("g-canary", tp0);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(counts.get("a1")).isEqualTo(1));
            log.info("[断言] partition=canary#0 messageId=a1 判定=恢复后从暂停位置继续，恰好一次");
        }
    }
}
