package com.github.lisama569.localmessagequeuejava.mq.store;

import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

/**
 * 基于文件的已确认位点存储：每分区一个小文件，临时文件 + 原子 move + fsync，
 * 进程中断后要么读到旧值、要么读到新值，不出现半写状态。
 */
public class FileAckWatermarkStore implements AckWatermarkStore {

    private final Path rootDir;

    public FileAckWatermarkStore(Path rootDir) {
        this.rootDir = rootDir;
        try {
            Files.createDirectories(rootDir);
        } catch (IOException e) {
            throw new UncheckedIOException("无法创建确认水位目录: " + rootDir, e);
        }
    }

    private Path fileOf(TopicPartition tp) {
        String name = tp.topic().replaceAll("[^a-zA-Z0-9._-]", "_") + "-" + tp.partition();
        return rootDir.resolve(name);
    }

    @Override
    public synchronized long load(TopicPartition tp) {
        Path file = fileOf(tp);
        if (!Files.exists(file)) {
            return 0;
        }
        try {
            String text = Files.readString(file, StandardCharsets.UTF_8).trim();
            return text.isEmpty() ? 0 : Long.parseLong(text);
        } catch (IOException e) {
            throw new UncheckedIOException("加载确认水位失败: " + tp, e);
        }
    }

    @Override
    public synchronized void store(TopicPartition tp, long committedOffset) {
        Path file = fileOf(tp);
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            Files.createDirectories(rootDir);
            Files.writeString(tmp, Long.toString(committedOffset), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ch.force(true);
            }
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("保存确认水位失败: " + tp, e);
        }
    }
}
