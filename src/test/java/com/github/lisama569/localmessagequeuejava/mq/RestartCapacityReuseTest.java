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
 * 确认完 → 重启 → 消费者尚未接管：已确认消息不再占容量。
 * 积压/剩余容量按持久化确认水位恢复，与物理日志是否裁剪、消费者是否在线无关；
 * 多分区各自独立；QUEUE_FULL / SHUTTING_DOWN 拒绝原因分类保持不变。
 */
class RestartCapacityReuseTest {

    private static final Logger log = LoggerFactory.getLogger(RestartCapacityReuseTest.class);

    @TempDir
    Path dataDir;

    @Test
    void ackedCapacityReusableAfterRestartWithoutConsumerAndWithoutTruncation() throws Exception {
        TopicPartition tp = new TopicPartition("rc", 0);

        // 阶段一：打满容量 → 全部确认（不开启裁剪，物理日志原样保留）→ 停掉进程
        try (MqService service1 = new MqService(dataDir, 3)) {
            for (int i = 0; i < 3; i++) {
                var m = service1.send("rc", 0, "r" + i, "p" + i);
                log.info("[发送] partition=rc#0 offset={} messageId={} backlog={}",
                        m.offset(), m.id(), service1.watermark(tp).backlog());
            }
            CountDownLatch consumed = new CountDownLatch(3);
            service1.registerConsumer("g-rc", msg -> {
                log.info("[消费] partition=rc#0 offset={} messageId={} 判定=确认即释放容量",
                        msg.offset(), msg.id());
                consumed.countDown();
            }, ConsumerOptions.defaults().pollInterval(Duration.ofMillis(10)), List.of(tp));
            assertThat(consumed.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service1.committedOffsets("g-rc").get(tp)).isEqualTo(3));
            PartitionWatermark wm = service1.watermark(tp);
            log.info("[阶段一] partition=rc#0 committed=3 backlog={} remaining={} 判定=确认完容量已腾空，进程退出",
                    wm.backlog(), wm.remaining());
            assertThat(wm.backlog()).isEqualTo(0);
        }

        // 阶段二：同一份数据重新打开，不接入任何消费者；物理日志未裁剪（base=0, next=3）
        try (MqService service2 = new MqService(dataDir, 3)) {
            log.info("[重启] partition=rc#0 base={} next={} 判定=物理日志保留但消息均已确认",
                    service2.queue().baseOffset(tp), service2.queue().nextOffset(tp));
            assertThat(service2.queue().nextOffset(tp)).isEqualTo(3);

            PartitionWatermark wm = service2.watermark(tp);
            log.info("[断言] partition=rc#0 backlog={} capacity={} remaining={} 判定=无消费者时积压即真实未确认条数",
                    wm.backlog(), wm.capacity(), wm.remaining());
            assertThat(wm.backlog()).isEqualTo(0);
            assertThat(wm.remaining()).isEqualTo(3);
            assertThat(service2.queue().backlog(tp)).isEqualTo(0);

            // 容量立即复用：新消息正常接收，位点在已确认历史之后继续分配
            for (int i = 3; i < 6; i++) {
                var m = service2.send("rc", 0, "r" + i, "p" + i);
                log.info("[发送] partition=rc#0 offset={} messageId={} 判定=重启后容量复用接收成功", m.offset(), m.id());
            }
            // 新消息未确认，再次打满：拒绝原因仍是 QUEUE_FULL 而非误拒
            assertThatThrownBy(() -> service2.send("rc", 0, "rX", "pX"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        assertThat(e.getReason()).isEqualTo(RejectReason.QUEUE_FULL);
                        log.info("[断言] partition=rc#0 messageId=rX reason={} 判定=新消息未确认占满容量，拒绝原因分类不变",
                                e.getReason());
                    });
        }
    }

    @Test
    void perPartitionIsolationAndRejectReasonsAfterRestart() throws Exception {
        TopicPartition p0 = new TopicPartition("iso", 0);
        TopicPartition p1 = new TopicPartition("iso", 1);

        // 阶段一：两个分区都打满；消费者只接管 p0 并确认完，p1 保持未确认
        try (MqService service1 = new MqService(dataDir, 2)) {
            for (int i = 0; i < 2; i++) {
                service1.send("iso", 0, "a" + i, "p" + i);
                service1.send("iso", 1, "b" + i, "p" + i);
            }
            CountDownLatch consumed = new CountDownLatch(2);
            service1.registerConsumer("g-iso", msg -> {
                log.info("[消费] partition={} offset={} messageId={} 判定=仅 p0 被确认",
                        msg.topic() + "#" + msg.partition(), msg.offset(), msg.id());
                consumed.countDown();
            }, ConsumerOptions.defaults().pollInterval(Duration.ofMillis(10)), List.of(p0));
            assertThat(consumed.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service1.committedOffsets("g-iso").get(p0)).isEqualTo(2));
            log.info("[阶段一] partition=iso#0 committed=2 partition=iso#1 committed=0 判定=p0 腾空/p1 占满，进程退出");
        }

        // 阶段二：重启后不接入消费者，各分区容量互不影响
        try (MqService service2 = new MqService(dataDir, 2)) {
            PartitionWatermark wm0 = service2.watermark(p0);
            PartitionWatermark wm1 = service2.watermark(p1);
            log.info("[断言] partition=iso#0 backlog={} remaining={} 判定=已确认分区容量复用",
                    wm0.backlog(), wm0.remaining());
            log.info("[断言] partition=iso#1 backlog={} remaining={} 判定=未确认分区容量仍被占用",
                    wm1.backlog(), wm1.remaining());
            assertThat(wm0.backlog()).isEqualTo(0);
            assertThat(wm0.remaining()).isEqualTo(2);
            assertThat(wm1.backlog()).isEqualTo(2);
            assertThat(wm1.remaining()).isEqualTo(0);

            var m = service2.send("iso", 0, "a2", "p2");
            log.info("[发送] partition=iso#0 offset={} messageId=a2 判定=已确认分区重启后可接收新消息", m.offset());
            assertThatThrownBy(() -> service2.send("iso", 1, "b2", "p2"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        assertThat(e.getReason()).isEqualTo(RejectReason.QUEUE_FULL);
                        log.info("[断言] partition=iso#1 messageId=b2 reason={} 判定=未确认分区仍按容量上限拒收",
                                e.getReason());
                    });

            // 关停后拒绝原因仍是 SHUTTING_DOWN，与 QUEUE_FULL 明确区分
            service2.close();
            assertThatThrownBy(() -> service2.send("iso", 0, "aZ", "pZ"))
                    .isInstanceOfSatisfying(MessageRejectedException.class, e -> {
                        assertThat(e.getReason()).isEqualTo(RejectReason.SHUTTING_DOWN);
                        log.info("[断言] partition=iso#0 messageId=aZ reason={} 判定=关停拒绝与容量拒绝分类不混淆",
                                e.getReason());
                    });
        }
    }

    @Test
    void ackedCapacityReusableAfterRestartWithTruncationEnabled() throws Exception {
        TopicPartition tp = new TopicPartition("rt", 0);

        // 阶段一：开启裁剪，确认后物理日志被截短；停掉进程
        try (MqService service1 = new MqService(dataDir, 2)) {
            service1.send("rt", 0, "t0", "p0");
            service1.send("rt", 0, "t1", "p1");
            CountDownLatch consumed = new CountDownLatch(2);
            service1.registerConsumer("g-rt", msg -> {
                log.info("[消费] partition=rt#0 offset={} messageId={} 判定=确认并裁剪物理日志",
                        msg.offset(), msg.id());
                consumed.countDown();
            }, ConsumerOptions.defaults().pollInterval(Duration.ofMillis(10))
                    .truncateCommittedLog(true), List.of(tp));
            assertThat(consumed.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service1.committedOffsets("g-rt").get(tp)).isEqualTo(2));
            log.info("[阶段一] partition=rt#0 committed=2 base={} 判定=已确认日志已裁剪，进程退出",
                    service1.queue().baseOffset(tp));
        }

        // 阶段二：重启后不接入消费者，裁剪过与未裁剪场景的容量口径一致
        try (MqService service2 = new MqService(dataDir, 2)) {
            PartitionWatermark wm = service2.watermark(tp);
            log.info("[断言] partition=rt#0 base={} backlog={} remaining={} 判定=裁剪后重启容量同样立即复用",
                    service2.queue().baseOffset(tp), wm.backlog(), wm.remaining());
            assertThat(wm.backlog()).isEqualTo(0);
            assertThat(wm.remaining()).isEqualTo(2);
            var m = service2.send("rt", 0, "t2", "p2");
            log.info("[发送] partition=rt#0 offset={} messageId=t2 判定=重启后接收成功", m.offset());
            assertThat(m.offset()).isEqualTo(2);
        }
    }
}
