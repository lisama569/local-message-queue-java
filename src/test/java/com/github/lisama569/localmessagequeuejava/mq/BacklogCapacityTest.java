package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.consumer.ConsumerOptions;
import com.github.lisama569.localmessagequeuejava.mq.exception.MessageRejectedException;
import com.github.lisama569.localmessagequeuejava.mq.model.PartitionWatermark;
import com.github.lisama569.localmessagequeuejava.mq.model.RejectReason;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * 积压与容量水位：
 * <ul>
 *   <li>积压按未确认消息实时计算，确认后立即释放容量，无需物理裁剪日志；</li>
 *   <li>水位查询可按分区看到积压条数、已确认位点、距容量上限剩余条数；</li>
 *   <li>水位与暂停状态跨重启保持；容量拒收原因仍为 QUEUE_FULL（与关停拒绝区分）。</li>
 * </ul>
 */
class BacklogCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(BacklogCapacityTest.class);

    @TempDir
    Path dataDir;

    @Test
    void ackReleasesCapacityImmediatelyAndWatermarkQueryable() throws Exception {
        TopicPartition tp0 = new TopicPartition("wl", 0);
        TopicPartition tp1 = new TopicPartition("wl", 1);
        long capacity = 3;

        try (MqService service = new MqService(dataDir.resolve("immediate"), capacity)) {
            CountDownLatch consumed = new CountDownLatch(6);
            // 关键：不开启 truncateCommittedLog，容量释放只应依赖确认水位
            service.registerConsumer("g-wl", msg -> consumed.countDown(),
                    ConsumerOptions.defaults(), List.of(tp0, tp1));

            for (int i = 0; i < capacity; i++) {
                service.send("wl", 0, "a" + i, "p" + i);
            }
            PartitionWatermark full = service.watermark("g-wl", tp0);
            assertThat(full.backlog()).isEqualTo(3);
            assertThat(full.remaining()).isZero();
            assertThat(full.capacity()).isEqualTo(3);
            log.info("[水位] partition=wl#0 backlog={} remaining={} committed={} next={} 判定=打满容量",
                    full.backlog(), full.remaining(), full.committedOffset(), full.nextOffset());

            assertThatThrownBy(() -> service.send("wl", 0, "aX", "pX"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        assertThat(e.getReason()).isEqualTo(RejectReason.QUEUE_FULL);
                        log.info("[断言] partition=wl#0 messageId=aX reason={} 判定=未确认积压打满被拒收",
                                e.getReason());
                    });

            // 另一分区不受影响，容量独立
            service.send("wl", 1, "b0", "q0");
            PartitionWatermark other = service.watermark("g-wl", tp1);
            assertThat(other.remaining()).isEqualTo(2);
            log.info("[水位] partition=wl#1 backlog={} remaining={} 判定=分区间容量互不影响",
                    other.backlog(), other.remaining());

            // 确认后容量立刻复用（不依赖物理裁剪）
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(service.queue().backlog(tp0)).isZero());
            for (int i = 3; i < 6; i++) {
                var msg = service.send("wl", 0, "a" + i, "p" + i);
                log.info("[发送] partition=wl#0 offset={} messageId=a{} 判定=积压清空后容量立即复用",
                        msg.offset(), i);
            }
            assertThat(consumed.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                PartitionWatermark w = service.watermark("g-wl", tp0);
                assertThat(w.backlog()).isZero();
                assertThat(w.remaining()).isEqualTo(capacity);
                assertThat(w.committedOffset()).isEqualTo(6);
                assertThat(w.baseOffset()).isZero(); // 未做物理裁剪，base 仍为 0
            });
            log.info("[断言] partition=wl#0 committedOffset=6 backlog=0 remaining=3 base=0 判定=水位口径释放容量");
        }
    }

    @Test
    void backlogWatermarkAndPauseSurviveRestart() throws Exception {
        TopicPartition tp = new TopicPartition("persist", 0);

        try (MqService service1 = new MqService(dataDir.resolve("restart"), 100)) {
            CountDownLatch oneDone = new CountDownLatch(1);
            service1.registerConsumer("g-ps", msg -> oneDone.countDown(),
                    ConsumerOptions.defaults(), List.of(tp));
            service1.send("persist", 0, "s0", "p0");
            assertThat(oneDone.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service1.queue().backlog(tp)).isZero());
            // 先暂停，再发 s1：确定性地制造“暂停中积压 1 条未确认”
            service1.pause("g-ps", tp);
            service1.send("persist", 0, "s1", "p1");
            Thread.sleep(300);
            assertThat(service1.queue().backlog(tp)).isEqualTo(1);
            log.info("[阶段一] partition=persist#0 committed=1 backlog=1 paused=true 判定=状态落盘后重启");
        }

        try (MqService service2 = new MqService(dataDir.resolve("restart"), 100)) {
            // 尚未注册消费组时也能按队列已确认水位查到积压（paused 无消费组视角，恒 false）
            PartitionWatermark beforeRegister = service2.watermark("g-ps", tp);
            assertThat(beforeRegister.backlog()).isEqualTo(1);
            assertThat(beforeRegister.committedOffset()).isEqualTo(1);
            assertThat(beforeRegister.remaining()).isEqualTo(99);
            log.info("[水位] partition=persist#0 backlog={} committed={} remaining={} 判定=水位跨重启恢复",
                    beforeRegister.backlog(), beforeRegister.committedOffset(), beforeRegister.remaining());

            CountDownLatch done = new CountDownLatch(1);
            service2.registerConsumer("g-ps", msg -> {
                log.info("[消费] partition=persist#0 offset={} messageId={} 判定=重启后恢复消费",
                        msg.offset(), msg.id());
                done.countDown();
            }, ConsumerOptions.defaults(), List.of(tp));

            assertThat(service2.isPaused("g-ps", tp))
                    .as("暂停标志随消费组状态持久化，重启后仍暂停")
                    .isTrue();
            boolean consumedWhilePaused = done.await(800, TimeUnit.MILLISECONDS);
            assertThat(consumedWhilePaused).as("重启后暂停仍生效，s1 不得投递").isFalse();
            log.info("[断言] partition=persist#0 messageId=s1 判定=重启后暂停生效，未投递");

            service2.resume("g-ps", tp);
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service2.queue().backlog(tp)).isZero());
            PartitionWatermark w = service2.watermark("g-ps", tp);
            assertThat(w.remaining()).isEqualTo(100);
            assertThat(w.paused()).isFalse();
            log.info("[断言] partition=persist#0 backlog=0 remaining=100 paused=false 判定=恢复消费并释放容量");
        }
    }
}
