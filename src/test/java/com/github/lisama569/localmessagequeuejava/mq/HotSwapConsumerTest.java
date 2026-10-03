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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * 订阅者热替换：切换窗口内同一分区不被新老订阅者同时处理；
 * 未确认消息不丢、已确认消息不重复产生副作用；新订阅者从未确认位置继续。
 */
class HotSwapConsumerTest {

    private static final Logger log = LoggerFactory.getLogger(HotSwapConsumerTest.class);

    @TempDir
    Path dataDir;

    @Test
    void hotSwapContinuesFromUnackedPositionWithoutLossOrDuplicate() throws Exception {
        TopicPartition tp = new TopicPartition("sw", 0);
        int total = 20;
        Map<String, String> handledBy = new ConcurrentHashMap<>();
        Map<String, Integer> sideEffectCount = new ConcurrentHashMap<>();
        List<String> orderedIds = new CopyOnWriteArrayList<>();
        AtomicInteger oldInvocationsOnInFlight = new AtomicInteger();
        CountDownLatch firstAcked = new CountDownLatch(5);
        // s5 在旧订阅者处“处理中”挂住：已投递、副作用未完成、确认未落盘
        CountDownLatch s5Reached = new CountDownLatch(1);
        CountDownLatch releaseS5 = new CountDownLatch(1);

        try (MqService service = new MqService(dataDir, 1000)) {
            for (int i = 0; i < total; i++) {
                Message msg = service.send("sw", 0, "s" + i, "p" + i);
                log.info("[发送] partition=sw#0 offset={} messageId={}", msg.offset(), msg.id());
            }

            ConsumerOptions opts = ConsumerOptions.defaults()
                    .pollInterval(Duration.ofMillis(10))
                    .processingTimeout(Duration.ofSeconds(2));
            service.registerConsumer("g-swap", msg -> {
                if (msg.id().equals("s5")) {
                    oldInvocationsOnInFlight.incrementAndGet();
                    log.info("[旧订阅者] partition=sw#0 offset=5 messageId=s5 判定=处理中挂起（副作用未完成）");
                    s5Reached.countDown();
                    try {
                        releaseS5.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        // 关停/超时中断线程：副作用仍未发生
                        Thread.currentThread().interrupt();
                    }
                    return;
                }
                handledBy.put(msg.id(), "old");
                sideEffectCount.merge(msg.id(), 1, Integer::sum);
                orderedIds.add(msg.id());
                log.info("[旧订阅者] partition=sw#0 offset={} messageId={} 判定=旧逻辑确认",
                        msg.offset(), msg.id());
                firstAcked.countDown();
            }, opts, List.of(tp));

            assertThat(firstAcked.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(s5Reached.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-swap").get(tp)).isEqualTo(5));
            log.info("[断言] partition=sw#0 committed=5 messageId=s5 判定=热替换前 s0-s4 已确认，s5 未确认处理中");

            // 热替换：旧订阅者先被关停（s5 的在途处理被超时中断、未落确认），新订阅者随后接管
            service.replaceConsumer("g-swap", msg -> {
                String previous = handledBy.putIfAbsent(msg.id(), "new");
                assertThat(previous)
                        .as("切换窗口内消息 %s 不得被新老订阅者各完成一次副作用", msg.id()).isNull();
                sideEffectCount.merge(msg.id(), 1, Integer::sum);
                orderedIds.add(msg.id());
                log.info("[新订阅者] partition=sw#0 offset={} messageId={} 判定=新逻辑接管确认",
                        msg.offset(), msg.id());
            }, opts, null);
            releaseS5.countDown();

            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-swap").get(tp)).isEqualTo(total));

            for (int i = 0; i < total; i++) {
                assertThat(sideEffectCount.get("s" + i))
                        .as("消息 s%d 必须恰好执行一次副作用（未确认不丢、已确认不重）", i).isEqualTo(1);
            }
            assertThat(orderedIds).containsExactly(java.util.stream.IntStream.range(0, total)
                    .mapToObj(i -> "s" + i).toArray(String[]::new));
            log.info("[断言] partition=sw#0 完整顺序={} 判定=不重不漏且分区内顺序不变", orderedIds);

            long oldCount = handledBy.values().stream().filter("old"::equals).count();
            long newCount = total - oldCount;
            log.info("[断言] partition=sw#0 旧确认={} 新确认={} 判定=未确认的 s5 起由新订阅者继续",
                    oldCount, newCount);
            assertThat(oldCount).isEqualTo(5);
            assertThat(newCount).isEqualTo(total - 5);
            assertThat(handledBy.get("s5")).isEqualTo("new");
            assertThat(oldInvocationsOnInFlight.get())
                    .as("s5 在旧订阅者处只投递过一次，新订阅者接管后不应出现并发双跑").isEqualTo(1);
        }
    }

    @Test
    void duplicateRegisterIsRejectedButReplaceSucceeds() throws Exception {
        TopicPartition tp = new TopicPartition("sw2", 0);
        CountDownLatch one = new CountDownLatch(1);
        try (MqService service = new MqService(dataDir.resolve("dup"), 100)) {
            service.registerConsumer("g", msg -> {
                log.info("[旧] partition=sw2#0 offset={} messageId={}", msg.offset(), msg.id());
                one.countDown();
            }, ConsumerOptions.defaults(), List.of(tp));
            service.send("sw2", 0, "d0", "p");
            assertThat(one.await(10, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() -> service.registerConsumer("g",
                    msg -> log.info("[非法] messageId={}", msg.id()),
                    ConsumerOptions.defaults(), List.of(tp)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("replaceConsumer");
            log.info("[断言] group=g 判定=同组重复注册被拒绝，避免两个订阅者并存");

            service.replaceConsumer("g", msg ->
                    log.info("[新] partition=sw2#0 offset={} messageId={}", msg.offset(), msg.id()),
                    ConsumerOptions.defaults(), null);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g").get(tp)).isEqualTo(1));
            log.info("[断言] group=g partition=sw2#0 committed=1 判定=热替换后位点延续，已确认消息未重放");
        }
    }
}
