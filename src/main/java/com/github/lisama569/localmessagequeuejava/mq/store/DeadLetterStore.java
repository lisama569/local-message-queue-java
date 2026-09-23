package com.github.lisama569.localmessagequeuejava.mq.store;

import com.github.lisama569.localmessagequeuejava.mq.handler.Json;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 死信存储：追加写 JSON 行 + fsync，保留原始载荷与失败原因，重启后可查询、可重放。
 */
public class DeadLetterStore {

    private final Path file;
    private final List<DeadLetterRecord> records = new ArrayList<>();

    public DeadLetterStore(Path file) {
        this.file = file;
        if (Files.exists(file)) {
            try (var lines = Files.lines(file, StandardCharsets.UTF_8)) {
                lines.filter(l -> !l.isBlank())
                        .map(l -> Json.fromJson(l, DeadLetterRecord.class))
                        .forEach(records::add);
            } catch (IOException e) {
                throw new UncheckedIOException("加载死信失败: " + file, e);
            }
        }
    }

    public synchronized void add(DeadLetterRecord record) {
        try {
            Files.createDirectories(file.getParent());
            byte[] line = (Json.toJson(record) + "\n").getBytes(StandardCharsets.UTF_8);
            try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ch.write(java.nio.ByteBuffer.wrap(line));
                ch.force(true);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("写入死信失败: " + record.messageId(), e);
        }
        records.add(record);
    }

    public synchronized List<DeadLetterRecord> findAll() {
        return List.copyOf(records);
    }

    public synchronized Optional<DeadLetterRecord> find(String messageId) {
        return records.stream().filter(r -> r.messageId().equals(messageId)).findFirst();
    }
}
