package com.github.lisama569.localmessagequeuejava.mq.api;

import com.github.lisama569.localmessagequeuejava.mq.MqService;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.model.PartitionWatermark;
import com.github.lisama569.localmessagequeuejava.mq.model.ReplayOutcome;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 本地验证用 HTTP API：发送、死信查询/重放、位点查询。
 */
@RestController
@RequestMapping("/api/mq")
@ConditionalOnProperty(prefix = "mq", name = "enabled", havingValue = "true")
public class MqController {

    private final MqService mqService;

    public MqController(MqService mqService) {
        this.mqService = mqService;
    }

    public record SendRequest(String topic, int partition, String messageId, String payload) {
    }

    @PostMapping("/send")
    public Message send(@RequestBody SendRequest request) {
        return mqService.send(request.topic(), request.partition(), request.messageId(), request.payload());
    }

    @GetMapping("/dead-letters")
    public List<DeadLetterRecord> deadLetters(@RequestParam String group) {
        return mqService.deadLetters(group);
    }

    @PostMapping("/dead-letters/replay")
    public ReplayOutcome replay(@RequestParam String group, @RequestParam String messageId) {
        return mqService.replayDeadLetter(group, messageId);
    }

    @GetMapping("/offsets")
    public Map<String, Long> offsets(@RequestParam String group) {
        return mqService.committedOffsets(group).entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().key(), Map.Entry::getValue));
    }

    /**
     * 按分区查询积压与容量水位（未确认条数、已确认位点、剩余容量、暂停状态）。
     */
    @GetMapping("/backlog")
    public Map<String, PartitionWatermark> backlog(@RequestParam String group, @RequestParam String topic,
                                                   @RequestParam int partitions) {
        return java.util.stream.IntStream.range(0, partitions).boxed()
                .collect(Collectors.toMap(p -> topic + "#" + p,
                        p -> mqService.watermark(group, new TopicPartition(topic, p))));
    }

    /** 暂停某消费组在指定分区上的投递（幂等）。 */
    @PostMapping("/pause")
    public Map<String, Object> pause(@RequestParam String group, @RequestParam String topic,
                                     @RequestParam int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        mqService.pause(group, tp);
        return Map.of("group", group, "partition", tp.key(), "paused", true);
    }

    /** 恢复某消费组在指定分区上的投递（幂等）。 */
    @PostMapping("/resume")
    public Map<String, Object> resume(@RequestParam String group, @RequestParam String topic,
                                      @RequestParam int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        mqService.resume(group, tp);
        return Map.of("group", group, "partition", tp.key(), "paused", false);
    }
}
