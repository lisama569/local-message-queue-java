package com.github.lisama569.localmessagequeuejava.mq.store;

import com.github.lisama569.localmessagequeuejava.mq.handler.Json;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于文件的分区消息日志：每分区一个目录，data.log 追加写 + fsync，
 * base 文件记录起始偏移；重启后从磁盘恢复位点。
 */
public class FileMessageLogStore implements MessageLogStore {

    private final Path rootDir;
    private final Map<TopicPartition, PartitionLog> logs = new ConcurrentHashMap<>();

    public FileMessageLogStore(Path rootDir) {
        this.rootDir = rootDir;
        try {
            Files.createDirectories(rootDir);
        } catch (IOException e) {
            throw new UncheckedIOException("无法创建日志根目录: " + rootDir, e);
        }
    }

    private record PartitionLog(long baseOffset, long count) {
    }

    private Path partitionDir(TopicPartition tp) {
        return rootDir.resolve(sanitize(tp.topic()) + "-" + tp.partition());
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private PartitionLog state(TopicPartition tp) {
        return logs.computeIfAbsent(tp, this::loadState);
    }

    private PartitionLog loadState(TopicPartition tp) {
        Path dir = partitionDir(tp);
        Path data = dir.resolve("data.log");
        Path base = dir.resolve("base");
        try {
            long baseOffset = 0;
            if (Files.exists(base)) {
                baseOffset = Long.parseLong(Files.readString(base, StandardCharsets.UTF_8).trim());
            }
            long count = 0;
            if (Files.exists(data)) {
                try (var lines = Files.lines(data, StandardCharsets.UTF_8)) {
                    count = lines.filter(l -> !l.isBlank()).count();
                }
            }
            return new PartitionLog(baseOffset, count);
        } catch (IOException e) {
            throw new UncheckedIOException("加载分区日志状态失败: " + tp, e);
        }
    }

    @Override
    public synchronized long append(TopicPartition tp, String messageId, String payload) {
        PartitionLog st = state(tp);
        long offset = st.baseOffset() + st.count();
        LogEntry entry = new LogEntry(offset, messageId, payload, System.currentTimeMillis());
        Path dir = partitionDir(tp);
        try {
            Files.createDirectories(dir);
            Path data = dir.resolve("data.log");
            byte[] line = (Json.toJson(entry) + "\n").getBytes(StandardCharsets.UTF_8);
            try (FileChannel ch = FileChannel.open(data, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ch.write(java.nio.ByteBuffer.wrap(line));
                ch.force(true);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("追加消息失败: " + tp + " id=" + messageId, e);
        }
        logs.put(tp, new PartitionLog(st.baseOffset(), st.count() + 1));
        return offset;
    }

    @Override
    public synchronized List<LogEntry> read(TopicPartition tp, long fromOffset, long toOffset) {
        PartitionLog st = state(tp);
        List<LogEntry> result = new ArrayList<>();
        Path data = partitionDir(tp).resolve("data.log");
        if (!Files.exists(data)) {
            return result;
        }
        try (var lines = Files.lines(data, StandardCharsets.UTF_8)) {
            long[] idx = {0};
            lines.filter(l -> !l.isBlank()).forEach(line -> {
                long offset = st.baseOffset() + idx[0]++;
                if (offset >= fromOffset && offset < toOffset) {
                    result.add(Json.fromJson(line, LogEntry.class));
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("读取消息失败: " + tp, e);
        }
        return result;
    }

    @Override
    public long baseOffset(TopicPartition tp) {
        return state(tp).baseOffset();
    }

    @Override
    public long nextOffset(TopicPartition tp) {
        PartitionLog st = state(tp);
        return st.baseOffset() + st.count();
    }

    @Override
    public long size(TopicPartition tp) {
        return state(tp).count();
    }

    @Override
    public synchronized void truncateBefore(TopicPartition tp, long truncateBefore) {
        PartitionLog st = state(tp);
        if (truncateBefore <= st.baseOffset()) {
            return;
        }
        Path dir = partitionDir(tp);
        Path data = dir.resolve("data.log");
        Path tmp = dir.resolve("data.log.tmp");
        List<String> kept = new ArrayList<>();
        long newBase = st.baseOffset();
        if (Files.exists(data)) {
            try (var lines = Files.lines(data, StandardCharsets.UTF_8)) {
                long[] idx = {0};
                lines.filter(l -> !l.isBlank()).forEach(line -> {
                    long offset = st.baseOffset() + idx[0]++;
                    if (offset >= truncateBefore) {
                        kept.add(line);
                    }
                });
            } catch (IOException e) {
                throw new UncheckedIOException("裁剪日志失败: " + tp, e);
            }
            newBase = Math.max(st.baseOffset(), truncateBefore);
        }
        try {
            Files.createDirectories(dir);
            Files.write(tmp, kept, StandardCharsets.UTF_8);
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ch.force(true);
            }
            Files.move(tmp, data, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            Files.writeString(dir.resolve("base"), Long.toString(newBase), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("裁剪日志落盘失败: " + tp, e);
        }
        logs.put(tp, new PartitionLog(newBase, kept.size()));
    }
}
