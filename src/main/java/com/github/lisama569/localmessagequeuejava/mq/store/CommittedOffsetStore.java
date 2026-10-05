package com.github.lisama569.localmessagequeuejava.mq.store;

import com.github.lisama569.localmessagequeuejava.mq.handler.Json;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 队列侧的分区确认水位存储：记录每个分区“已被确认到的位点”（单调不减）。
 * 临时文件 + 原子 move + fsync，进程中断后要么读到旧水位、要么读到新水位。
 * 重启后无需消费者接管，即可按 未确认 = nextOffset - max(确认水位, 物理起始位点) 恢复容量口径。
 */
public class CommittedOffsetStore {

    private final Path file;
    private final Map<TopicPartition, Long> watermarks = new HashMap<>();

    /** 磁盘结构：分区 key -> 已确认位点 */
    public record CommittedOffsetData(Map<String, Long> partitions) {
    }

    public CommittedOffsetStore(Path file) {
        this.file = file;
        if (Files.exists(file)) {
            try {
                CommittedOffsetData data = Json.fromJson(Files.readString(file, StandardCharsets.UTF_8),
                        CommittedOffsetData.class);
                if (data != null && data.partitions() != null) {
                    data.partitions().forEach((key, offset) -> {
                        int sep = key.lastIndexOf('#');
                        watermarks.put(new TopicPartition(key.substring(0, sep),
                                Integer.parseInt(key.substring(sep + 1))), offset);
                    });
                }
            } catch (IOException e) {
                throw new UncheckedIOException("加载分区确认水位失败: " + file, e);
            }
        }
    }

    /** 分区已确认到的位点；无记录返回 null（调用方按物理日志保守兜底）。 */
    public synchronized Long load(TopicPartition tp) {
        return watermarks.get(tp);
    }

    /**
     * 推进分区确认水位（单调不减，落后写入直接忽略）并原子落盘。
     *
     * @return 落盘后的水位
     */
    public synchronized long advance(TopicPartition tp, long committedOffset) {
        long current = watermarks.getOrDefault(tp, Long.MIN_VALUE);
        if (committedOffset <= current) {
            return current;
        }
        watermarks.put(tp, committedOffset);
        Map<String, Long> byKey = new HashMap<>();
        watermarks.forEach((k, v) -> byKey.put(k.key(), v));
        String json = Json.toJson(new CommittedOffsetData(byKey));
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(tmp, json, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ch.force(true);
            }
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("保存分区确认水位失败: " + file, e);
        }
        return committedOffset;
    }
}
