package com.github.lisama569.localmessagequeuejava.mq.store;

import com.github.lisama569.localmessagequeuejava.mq.handler.Json;
import com.github.lisama569.localmessagequeuejava.mq.model.PartitionState;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于 JSON 文件的消费者组状态存储：临时文件 + 原子 move + fsync，
 * 保证进程中断后要么读到旧状态、要么读到新状态，不出现半写状态。
 */
public class FileGroupStateStore implements GroupStateStore {

    private final Path stateFile;

    /** 磁盘结构：分区 key -> 分区状态 */
    public record GroupStateData(Map<String, PartitionState> partitions) {
    }

    public FileGroupStateStore(Path stateFile) {
        this.stateFile = stateFile;
    }

    @Override
    public synchronized Map<TopicPartition, PartitionState> loadAll() {
        if (!Files.exists(stateFile)) {
            return Map.of();
        }
        try {
            GroupStateData data = Json.fromJson(Files.readString(stateFile, StandardCharsets.UTF_8),
                    GroupStateData.class);
            Map<TopicPartition, PartitionState> result = new HashMap<>();
            if (data != null && data.partitions() != null) {
                data.partitions().forEach((key, state) -> {
                    int sep = key.lastIndexOf('#');
                    result.put(new TopicPartition(key.substring(0, sep),
                            Integer.parseInt(key.substring(sep + 1))), state);
                });
            }
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException("加载消费组状态失败: " + stateFile, e);
        }
    }

    @Override
    public PartitionState load(TopicPartition tp) {
        return loadAll().getOrDefault(tp, PartitionState.init(0));
    }

    @Override
    public synchronized void saveAll(Map<TopicPartition, PartitionState> states) {
        Map<String, PartitionState> byKey = new HashMap<>();
        states.forEach((tp, state) -> byKey.put(tp.key(), state));
        String json = Json.toJson(new GroupStateData(byKey));
        Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
        try {
            Files.createDirectories(stateFile.getParent());
            Files.writeString(tmp, json, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ch.force(true);
            }
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("保存消费组状态失败: " + stateFile, e);
        }
    }
}
