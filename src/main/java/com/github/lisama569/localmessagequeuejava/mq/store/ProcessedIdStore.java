package com.github.lisama569.localmessagequeuejava.mq.store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashSet;
import java.util.Set;

/**
 * 已确认消息标识集合：追加写 + fsync，重启后恢复；
 * 用于保证重复投递与死信重放不产生重复副作用。
 */
public class ProcessedIdStore {

    private final Path file;
    private final Set<String> ids = new HashSet<>();

    public ProcessedIdStore(Path file) {
        this.file = file;
        if (Files.exists(file)) {
            try (var lines = Files.lines(file, StandardCharsets.UTF_8)) {
                lines.filter(l -> !l.isBlank()).forEach(ids::add);
            } catch (IOException e) {
                throw new UncheckedIOException("加载已确认消息标识失败: " + file, e);
            }
        }
    }

    public synchronized boolean contains(String messageId) {
        return ids.contains(messageId);
    }

    public synchronized void add(String messageId) {
        if (!ids.add(messageId)) {
            return;
        }
        try {
            Files.createDirectories(file.getParent());
            byte[] line = (messageId + "\n").getBytes(StandardCharsets.UTF_8);
            try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ch.write(java.nio.ByteBuffer.wrap(line));
                ch.force(true);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("记录已确认消息标识失败: " + messageId, e);
        }
    }

    public synchronized int size() {
        return ids.size();
    }
}
