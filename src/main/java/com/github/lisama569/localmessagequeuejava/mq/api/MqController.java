package com.github.lisama569.localmessagequeuejava.mq.api;

import com.github.lisama569.localmessagequeuejava.mq.MqService;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
import com.github.lisama569.localmessagequeuejava.mq.model.PartitionWatermark;
import com.github.lisama569.localmessagequeuejava.mq.model.ReplayOutcome;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
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

    @GetMapping("/backlog")
    public Map<String, Long> backlog(@RequestParam String topic, @RequestParam int partitions) {
        return java.util.stream.IntStream.range(0, partitions).boxed()
                .collect(Collectors.toMap(p -> topic + "#" + p,
                        p -> mqService.queue().backlog(new TopicPartition(topic, p))));
    }

    /**
     * 积压/容量水位：返回每分区未确认条数 backlog、容量上限 capacity、剩余可用 remaining。
     */
    @GetMapping("/watermarks")
    public Map<String, PartitionWatermark> watermarks() {
        return mqService.watermarks().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().key(), Map.Entry::getValue,
                        (a, b) -> a, LinkedHashMap::new));
    }

    /** 暂停指定消费组在某分区上的投递（可重复调用，返回状态是否发生变化）。 */
    @PostMapping("/pause")
    public Map<String, Object> pause(@RequestParam String group, @RequestParam String topic,
                                     @RequestParam int partition) {
        boolean changed = mqService.pause(group, new TopicPartition(topic, partition));
        return Map.of("group", group, "partition", topic + "#" + partition, "paused", true,
                "changed", changed);
    }

    /** 恢复指定消费组在某分区上的投递（可重复调用，返回状态是否发生变化）。 */
    @PostMapping("/resume")
    public Map<String, Object> resume(@RequestParam String group, @RequestParam String topic,
                                      @RequestParam int partition) {
        boolean changed = mqService.resume(group, new TopicPartition(topic, partition));
        return Map.of("group", group, "partition", topic + "#" + partition, "paused", false,
                "changed", changed);
    }
}
