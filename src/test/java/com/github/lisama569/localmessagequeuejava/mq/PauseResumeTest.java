package com.github.lisama569.localmessagequeuejava.mq;

import com.github.lisama569.localmessagequeuejava.mq.consumer.ConsumerOptions;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.model.PartitionWatermark;
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
import static org.awaitility.Awaitility.await;

/**
 * 按分区暂停/恢复投递：
 * 暂停期间不投递新消息，未确认状态与顺序保留；恢复后从原位点继续、不跳消息；
 * 其他分区照常并发推进；反复暂停/恢复结果稳定。
 */
class PauseResumeTest {

    private static final Logger log = LoggerFactory.getLogger(PauseResumeTest.class);

    @TempDir
    Path dataDir;

    @Test
    void pauseHoldsPositionAndResumeContinuesInOrderWithPartitionIsolation() throws Exception {
        String topic = "pipe";
        TopicPartition p0 = new TopicPartition(topic, 0);
        TopicPartition p1 = new TopicPartition(topic, 1);
        Map<Integer, List<Long>> offsetsByPartition = new ConcurrentHashMap<>();
        Map<Integer, List<String>> idsByPartition = new ConcurrentHashMap<>();
        // p0 处理到 m0 后挂住，以便在“有未确认消息”的状态下暂停
        CountDownLatch m0Reached = new CountDownLatch(1);
        CountDownLatch releaseP0 = new CountDownLatch(1);

        try (MqService service = new MqService(dataDir, 1000)) {
            for (int i = 0; i < 6; i++) {
                Message a = service.send(topic, 0, "a" + i, "pa" + i);
                Message b = service.send(topic, 1, "b" + i, "pb" + i);
                log.info("[发送] partition={}#0 offset={} messageId={}", topic, a.offset(), a.id());
                log.info("[发送] partition={}#1 offset={} messageId={}", topic, b.offset(), b.id());
            }

            service.registerConsumer("g-pause", msg -> {
                if (msg.partition() == 0 && msg.id().equals("a0")) {
                    log.info("[闸门] partition=pipe#0 offset=0 messageId=a0 判定=处理中挂住以制造未确认窗口");
                    m0Reached.countDown();
                    try {
                        releaseP0.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                offsetsByPartition.computeIfAbsent(msg.partition(), k -> new CopyOnWriteArrayList<>())
                        .add(msg.offset());
                idsByPartition.computeIfAbsent(msg.partition(), k -> new CopyOnWriteArrayList<>())
                        .add(msg.id());
                log.info("[消费] partition=pipe#{} offset={} messageId={}",
                        msg.partition(), msg.offset(), msg.id());
            }, ConsumerOptions.defaults().pollInterval(Duration.ofMillis(10)), List.of(p0, p1));

            assertThat(m0Reached.await(10, TimeUnit.SECONDS)).isTrue();

            // 暂停 p0（a0 在途），放开闸门让在途完成；之后 p0 必须停在 a0，不再投递 a1+
            assertThat(service.pause("g-pause", p0)).isTrue();
            log.info("[断言] partition=pipe#0 paused=true 判定=暂停投递（a0 仍为未确认队头，在途可完成）");
            // 重复暂停：状态不变、结果稳定
            assertThat(service.pause("g-pause", p0))
                    .as("重复暂停必须幂等，返回 false 表示状态未变化").isFalse();
            releaseP0.countDown();

            // p1 不受影响，6 条照常并发消费完
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-pause").get(p1)).isEqualTo(6));
            log.info("[断言] partition=pipe#1 committed=6 判定=暂停 p0 不影响其他分区并发推进");

            // 给 p0 足够的调度时间，确认它停在 1 且水位反映未确认积压
            Thread.sleep(300);
            assertThat(service.committedOffsets("g-pause").get(p0))
                    .as("暂停期间 p0 不得越过 a0 继续投递").isEqualTo(1);
            PartitionWatermark pausedWatermark = service.watermark(p0);
            assertThat(pausedWatermark.backlog())
                    .as("暂停期间 a1-a5 仍占用容量，积压=5").isEqualTo(5);
            assertThat(pausedWatermark.remaining()).isEqualTo(pausedWatermark.capacity() - 5);
            log.info("[断言] partition=pipe#0 committed=1 backlog={} remaining={} 判定=暂停保留未确认状态与容量占用",
                    pausedWatermark.backlog(), pausedWatermark.remaining());

            // 暂停期间再发一条：可发送（占容量），但不会被投递
            Message extra = service.send(topic, 0, "a6", "pa6");
            log.info("[发送] partition=pipe#0 offset={} messageId=a6 判定=暂停期间发送成功但不投递", extra.offset());
            Thread.sleep(300);
            assertThat(service.committedOffsets("g-pause").get(p0)).isEqualTo(1);

            // 恢复：重复恢复幂等；恢复后严格按序继续，不跳过 a1-a6
            assertThat(service.resume("g-pause", p0)).isTrue();
            assertThat(service.resume("g-pause", p0))
                    .as("重复恢复必须幂等，返回 false 表示状态未变化").isFalse();
            log.info("[断言] partition=pipe#0 paused=false 判定=恢复投递，从 a1 原未确认位置继续");

            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-pause").get(p0)).isEqualTo(7));

            List<String> p0Ids = idsByPartition.get(0);
            assertThat(p0Ids).as("恢复后不得跳消息、不得乱序")
                    .containsExactly("a0", "a1", "a2", "a3", "a4", "a5", "a6");
            assertThat(offsetsByPartition.get(0)).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L);
            log.info("[断言] partition=pipe#0 处理序列={} 判定=暂停/恢复后顺序与位点完整", p0Ids);
            assertThat(idsByPartition.get(1)).containsExactly("b0", "b1", "b2", "b3", "b4", "b5");

            // 再暂停→再恢复一轮，结果稳定：无新消息时位点不动，恢复后仍可继续接收投递
            assertThat(service.pause("g-pause", p0)).isTrue();
            Thread.sleep(100);
            assertThat(service.committedOffsets("g-pause").get(p0)).isEqualTo(7);
            assertThat(service.resume("g-pause", p0)).isTrue();
            Message last = service.send(topic, 0, "a7", "pa7");
            log.info("[发送] partition=pipe#0 offset={} messageId=a7 判定=二次恢复后继续投递", last.offset());
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-pause").get(p0)).isEqualTo(8));
            assertThat(idsByPartition.get(0)).endsWith("a7");
            log.info("[断言] partition=pipe#0 committed=8 判定=反复暂停/恢复结果稳定，无失效无跳消息");
        }
    }
}
