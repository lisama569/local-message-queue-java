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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 暂停状态持久化：暂停后进程重启，新实例仍保持暂停、不投递；恢复后从原位点继续。
 */
class PauseRestartTest {

    private static final Logger log = LoggerFactory.getLogger(PauseRestartTest.class);

    @TempDir
    Path dataDir;

    @Test
    void pausedStateSurvivesRestart() throws Exception {
        TopicPartition tp = new TopicPartition("pr", 0);

        try (MqService service = new MqService(dataDir, 100)) {
            for (int i = 0; i < 4; i++) {
                service.send("pr", 0, "p" + i, "x");
            }
            var consumer = service.registerConsumer("g-pr",
                    msg -> log.info("[阶段一消费] partition=pr#0 offset={} messageId={}", msg.offset(), msg.id()),
                    ConsumerOptions.defaults(), List.of(tp));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service.committedOffsets("g-pr").get(tp)).isEqualTo(4));
            consumer.pause(tp);
            service.send("pr", 0, "p4", "x");
            log.info("[阶段一] partition=pr#0 paused=true 已发送 p4 判定=暂停态落盘后关闭进程");
        }

        try (MqService service2 = new MqService(dataDir, 100)) {
            List<String> consumed = new CopyOnWriteArrayList<>();
            var consumer2 = service2.registerConsumer("g-pr", msg -> {
                consumed.add(msg.id());
                log.info("[阶段二消费] partition=pr#0 offset={} messageId={}", msg.offset(), msg.id());
            }, ConsumerOptions.defaults().pollInterval(Duration.ofMillis(10)), List.of(tp));

            assertThat(consumer2.pausedPartitions()).contains(tp);
            log.info("[断言] partition=pr#0 paused=true 判定=暂停标记随状态文件恢复");
            TimeUnit.MILLISECONDS.sleep(400);
            assertThat(consumed).as("重启后仍处于暂停态，p4 不得被投递").isEmpty();
            assertThat(service2.committedOffsets("g-pr").get(tp)).isEqualTo(4);

            consumer2.resume(tp);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(service2.committedOffsets("g-pr").get(tp)).isEqualTo(5));
            assertThat(consumed).containsExactly("p4");
            log.info("[断言] partition=pr#0 恢复后序列={} 判定=从暂停前未确认位置继续，不丢消息", consumed);
        }
    }
}
