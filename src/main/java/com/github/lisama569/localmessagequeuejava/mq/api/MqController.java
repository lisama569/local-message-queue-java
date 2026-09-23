package com.github.lisama569.localmessagequeuejava.mq.api;

import com.github.lisama569.localmessagequeuejava.mq.MqService;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;
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

    @GetMapping("/backlog")
    public Map<String, Long> backlog(@RequestParam String topic, @RequestParam int partitions) {
        return java.util.stream.IntStream.range(0, partitions).boxed()
                .collect(Collectors.toMap(p -> topic + "#" + p,
                        p -> mqService.queue().backlog(new TopicPartition(topic, p))));
    }
}
