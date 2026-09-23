package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.consumer.ConsumerOptions;
import com.github.lisama569.localmessagequeuejava.mq.exception.MessageRejectedException;
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
 * 容量边界与关停拒绝：QUEUE_FULL 与 SHUTTING_DOWN 两种拒绝原因明确可区分；
 * 确认后的日志裁剪可释放容量。
 */
class CapacityBoundaryTest {

    private static final Logger log = LoggerFactory.getLogger(CapacityBoundaryTest.class);

    @TempDir
    Path dataDir;

    @Test
    void queueFullAndShutdownRejectionsAreDistinct() {
        try (MqService service = new MqService(dataDir.resolve("full"), 3)) {
            for (int i = 0; i < 3; i++) {
                var msg = service.send("cap", 0, "c" + i, "p" + i);
                log.info("[发送] partition=cap#0 offset={} messageId={}", msg.offset(), msg.id());
            }
            assertThatThrownBy(() -> service.send("cap", 0, "c3", "p3"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        log.info("[断言] partition=cap#0 messageId=c3 reason={} 判定=积压达到容量上限被拒绝",
                                e.getReason());
                        assertThat(e.getReason()).isEqualTo(RejectReason.QUEUE_FULL);
                    });

            service.close();
            assertThatThrownBy(() -> service.send("cap", 0, "c4", "p4"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        log.info("[断言] partition=cap#0 messageId=c4 reason={} 判定=关停期间拒绝新任务",
                                e.getReason());
                        assertThat(e.getReason()).isEqualTo(RejectReason.SHUTTING_DOWN);
                    });
            assertThat(RejectReason.QUEUE_FULL).isNotEqualTo(RejectReason.SHUTTING_DOWN);
        }
    }

    @Test
    void committedLogTruncationReleasesCapacity() throws Exception {
        TopicPartition tp = new TopicPartition("cap", 0);
        try (MqService service = new MqService(dataDir.resolve("release"), 3)) {
            CountDownLatch consumed = new CountDownLatch(6);
            service.registerConsumer("g-cap", msg -> {
                log.info("[消费] partition=cap#0 offset={} messageId={} 判定=确认后释放容量",
                        msg.offset(), msg.id());
                consumed.countDown();
            }, ConsumerOptions.defaults().truncateCommittedLog(true), List.of(tp));

            for (int i = 0; i < 3; i++) {
                service.send("cap", 0, "r" + i, "p" + i);
            }
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(service.queue().backlog(tp)).isEqualTo(0));
            log.info("[断言] partition=cap#0 backlog=0 判定=已确认日志被裁剪，容量释放");

            // 容量释放后可继续接收新消息
            for (int i = 3; i < 6; i++) {
                var msg = service.send("cap", 0, "r" + i, "p" + i);
                log.info("[发送] partition=cap#0 offset={} messageId={} 判定=容量释放后接收成功",
                        msg.offset(), msg.id());
            }
            assertThat(consumed.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-cap").get(tp)).isEqualTo(6));
        }
    }
}
