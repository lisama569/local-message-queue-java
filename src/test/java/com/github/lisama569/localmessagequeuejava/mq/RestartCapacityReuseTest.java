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
 * 确认完 → 重启 → 暂时不接入消费者：已确认消息释放的容量必须在重启后立即可复用，
 * 积压/剩余容量按真实未确认条数计算，与消费者是否在线、日志是否被物理裁剪无关；
 * 多分区各自独立；QUEUE_FULL / SHUTTING_DOWN 拒绝原因分类保持不变。
 */
class RestartCapacityReuseTest {

    private static final Logger log = LoggerFactory.getLogger(RestartCapacityReuseTest.class);

    @TempDir
    Path dataDir;

    @Test
    void ackedCapacityReusableAfterRestartWithoutConsumer() throws Exception {
        TopicPartition tp0 = new TopicPartition("rc", 0);
        TopicPartition tp1 = new TopicPartition("rc", 1);
        Path dir = dataDir.resolve("reuse");

        // 阶段一：rc#0 打满后全部确认（不裁剪物理日志）；rc#1 留 1 条未确认
        try (MqService service1 = new MqService(dir, 3)) {
            for (int i = 0; i < 3; i++) {
                var m = service1.send("rc", 0, "a" + i, "p" + i);
                log.info("[发送] partition=rc#0 offset={} messageId={}", m.offset(), m.id());
            }
            var b0 = service1.send("rc", 1, "b0", "q0");
            log.info("[发送] partition=rc#1 offset={} messageId={} 判定=留作未确认对照", b0.offset(), b0.id());

            CountDownLatch consumed = new CountDownLatch(3);
            // 只订阅 rc#0，且不开启 truncateCommittedLog：物理日志保留，验证容量释放与裁剪无关
            service1.registerConsumer("g-rc", msg -> {
                log.info("[消费] partition=rc#{} offset={} messageId={} 判定=确认即释放容量",
                        msg.partition(), msg.offset(), msg.id());
                consumed.countDown();
            }, ConsumerOptions.defaults().pollInterval(Duration.ofMillis(10)), List.of(tp0));

            assertThat(consumed.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service1.committedOffsets("g-rc").get(tp0)).isEqualTo(3));
            log.info("[阶段一] partition=rc#0 committed=3 backlog={} 判定=全部确认，等待重启",
                    service1.watermark(tp0).backlog());
        }

        // 阶段二：同一数据目录重启，不接入任何消费者，容量必须立即反映真实未确认条数
        try (MqService service2 = new MqService(dir, 3)) {
            PartitionWatermark wm0 = service2.watermark(tp0);
            PartitionWatermark wm1 = service2.watermark(tp1);
            log.info("[断言] partition=rc#0 backlog={} remaining={} committed=3 判定=重启后无消费者，已确认容量立即复用",
                    wm0.backlog(), wm0.remaining());
            log.info("[断言] partition=rc#1 backlog={} remaining={} 判定=分区隔离，未确认条数不受他区确认影响",
                    wm1.backlog(), wm1.remaining());
            assertThat(wm0.backlog()).isZero();
            assertThat(wm0.remaining()).isEqualTo(3);
            assertThat(service2.queue().backlog(tp0)).isZero();
            assertThat(wm1.backlog()).isEqualTo(1);
            assertThat(wm1.remaining()).isEqualTo(2);

            // rc#0 已确认容量可复用：3 条新消息全部接收，第 4 条按 QUEUE_FULL 拒绝
            for (int i = 0; i < 3; i++) {
                var m = service2.send("rc", 0, "c" + i, "p" + i);
                log.info("[发送] partition=rc#0 offset={} messageId={} 判定=重启后容量复用接收成功",
                        m.offset(), m.id());
            }
            assertThatThrownBy(() -> service2.send("rc", 0, "cX", "pX"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        log.info("[断言] partition=rc#0 messageId=cX reason={} 判定=复用后再次打满，拒绝原因不变",
                                e.getReason());
                        assertThat(e.getReason()).isEqualTo(RejectReason.QUEUE_FULL);
                    });

            // rc#1 仍有 1 条未确认：只能再收 2 条，第 3 条 QUEUE_FULL
            for (int i = 1; i <= 2; i++) {
                var m = service2.send("rc", 1, "b" + i, "q" + i);
                log.info("[发送] partition=rc#1 offset={} messageId={} 判定=分区独立计量接收成功",
                        m.offset(), m.id());
            }
            assertThatThrownBy(() -> service2.send("rc", 1, "bX", "qX"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        log.info("[断言] partition=rc#1 messageId=bX reason={} 判定=未确认积压仍占容量",
                                e.getReason());
                        assertThat(e.getReason()).isEqualTo(RejectReason.QUEUE_FULL);
                    });
        }
    }

    @Test
    void unackedBacklogSurvivesRestartAndRejectReasonsStayDistinct() {
        TopicPartition tp = new TopicPartition("rf", 0);
        Path dir = dataDir.resolve("full");

        // 阶段一：无消费者打满容量后关停
        try (MqService service1 = new MqService(dir, 2)) {
            for (int i = 0; i < 2; i++) {
                var m = service1.send("rf", 0, "u" + i, "p" + i);
                log.info("[发送] partition=rf#0 offset={} messageId={}", m.offset(), m.id());
            }
            log.info("[阶段一] partition=rf#0 backlog={} 判定=未确认打满，等待重启",
                    service1.watermark(tp).backlog());
        }

        // 阶段二：重启后不接入消费者，未确认积压必须原样保留，拒绝原因分类不变
        try (MqService service2 = new MqService(dir, 2)) {
            PartitionWatermark wm = service2.watermark(tp);
            log.info("[断言] partition=rf#0 backlog={} remaining={} 判定=未确认积压重启后不丢不占错",
                    wm.backlog(), wm.remaining());
            assertThat(wm.backlog()).isEqualTo(2);
            assertThat(wm.remaining()).isZero();

            assertThatThrownBy(() -> service2.send("rf", 0, "uX", "pX"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        log.info("[断言] partition=rf#0 messageId=uX reason={} 判定=真实打满仍按 QUEUE_FULL 拒绝",
                                e.getReason());
                        assertThat(e.getReason()).isEqualTo(RejectReason.QUEUE_FULL);
                    });

            service2.close();
            assertThatThrownBy(() -> service2.send("rf", 0, "uY", "pY"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        log.info("[断言] partition=rf#0 messageId=uY reason={} 判定=关停拒绝与容量拒绝可区分",
                                e.getReason());
                        assertThat(e.getReason()).isEqualTo(RejectReason.SHUTTING_DOWN);
                    });
            assertThat(RejectReason.QUEUE_FULL).isNotEqualTo(RejectReason.SHUTTING_DOWN);
        }
    }
}
