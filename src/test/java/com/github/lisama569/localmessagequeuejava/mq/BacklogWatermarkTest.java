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
 * 积压与容量水位：容量按未确认消息计；确认后容量立即释放可复用（不依赖日志裁剪）；
 * 水位按分区可查；QUEUE_FULL / SHUTTING_DOWN 拒绝原因分类保持不变。
 */
class BacklogWatermarkTest {

    private static final Logger log = LoggerFactory.getLogger(BacklogWatermarkTest.class);

    @TempDir
    Path dataDir;

    @Test
    void capacityReleasedImmediatelyOnAckWithoutTruncation() throws Exception {
        TopicPartition tp = new TopicPartition("wl", 0);
        try (MqService service = new MqService(dataDir.resolve("reuse"), 3)) {
            // 先在无消费者情况下打满容量，确定性验证 QUEUE_FULL
            for (int i = 0; i < 3; i++) {
                var m = service.send("wl", 0, "w" + i, "p" + i);
                log.info("[发送] partition=wl#0 offset={} messageId={} backlog={}",
                        m.offset(), m.id(), service.watermark(tp).backlog());
            }
            // 容量打满：第 4 条被 QUEUE_FULL 拒绝
            assertThatThrownBy(() -> service.send("wl", 0, "wX", "pX"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        assertThat(e.getReason()).isEqualTo(RejectReason.QUEUE_FULL);
                        log.info("[断言] partition=wl#0 messageId=wX reason={} 判定=未确认积压达到容量上限",
                                e.getReason());
                    });

            // 注册消费者（启动时按位点重建未确认计数），积压清空后容量必须立即复用
            CountDownLatch threeConsumed = new CountDownLatch(3);
            // 注意：不开启 truncateCommittedLog，物理日志仍保留，容量也必须能复用
            service.registerConsumer("g-wl", msg -> {
                log.info("[消费] partition=wl#0 offset={} messageId={} 判定=确认即释放容量",
                        msg.offset(), msg.id());
                threeConsumed.countDown();
            }, ConsumerOptions.defaults().pollInterval(Duration.ofMillis(10)), List.of(tp));

            assertThat(threeConsumed.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                PartitionWatermark wm = service.watermark(tp);
                assertThat(wm.backlog()).isEqualTo(0);
                assertThat(wm.remaining()).isEqualTo(wm.capacity());
            });
            PartitionWatermark cleared = service.watermark(tp);
            log.info("[断言] partition=wl#0 backlog={} capacity={} remaining={} 判定=确认完立即清空积压",
                    cleared.backlog(), cleared.capacity(), cleared.remaining());

            // 积压清空后立刻可以接收新消息（物理日志未裁剪，说明容量口径是“未确认”而非物理条数）
            for (int i = 3; i < 6; i++) {
                var m = service.send("wl", 0, "w" + i, "p" + i);
                log.info("[发送] partition=wl#0 offset={} messageId=w{} 判定=容量复用接收成功",
                        m.offset(), i);
            }
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-wl").get(tp)).isEqualTo(6));
            PartitionWatermark end = service.watermark(tp);
            assertThat(end.backlog()).isEqualTo(0);
            assertThat(end.remaining()).isEqualTo(3);
            log.info("[断言] partition=wl#0 committed=6 backlog=0 remaining=3 判定=容量可循环复用");
        }
    }

    @Test
    void watermarksArePerPartitionAndRejectReasonsStayDistinct() {
        TopicPartition p0 = new TopicPartition("wl2", 0);
        TopicPartition p1 = new TopicPartition("wl2", 1);
        try (MqService service = new MqService(dataDir.resolve("per"), 2)) {
            service.send("wl2", 0, "a0", "p");
            service.send("wl2", 0, "a1", "p");
            service.send("wl2", 1, "b0", "p");

            PartitionWatermark w0 = service.watermark(p0);
            PartitionWatermark w1 = service.watermark(p1);
            assertThat(w0.backlog()).isEqualTo(2);
            assertThat(w0.remaining()).isZero();
            assertThat(w1.backlog()).isEqualTo(1);
            assertThat(w1.remaining()).isEqualTo(1);
            log.info("[断言] partition=wl2#0 backlog={} remaining={} 判定=按分区独立计量",
                    w0.backlog(), w0.remaining());
            log.info("[断言] partition=wl2#1 backlog={} remaining={} 判定=其他分区容量不受影响",
                    w1.backlog(), w1.remaining());

            // 满的分区拒绝 QUEUE_FULL；另一个分区仍可接收
            assertThatThrownBy(() -> service.send("wl2", 0, "aX", "p"))
                    .isInstanceOf(MessageRejectedException.class)
                    .satisfies(e -> assertThat(((MessageRejectedException) e).getReason())
                            .isEqualTo(RejectReason.QUEUE_FULL));
            var ok = service.send("wl2", 1, "b1", "p");
            log.info("[发送] partition=wl2#1 offset={} messageId=b1 判定=未停的分区仍可接收", ok.offset());

            assertThat(service.watermarks()).containsKeys(p0, p1);

            service.close();
            assertThatThrownBy(() -> service.send("wl2", 1, "bZ", "p"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        assertThat(e.getReason()).isEqualTo(RejectReason.SHUTTING_DOWN);
                        log.info("[断言] messageId=bZ reason={} 判定=关停期间拒绝，原因分类与既有行为一致",
                                e.getReason());
                    });
        }
    }
}
