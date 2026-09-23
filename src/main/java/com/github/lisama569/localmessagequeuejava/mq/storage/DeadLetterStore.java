package com.github.lisama569.localmessagequeuejava.mq.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.localmessagequeuejava.mq.model.DeadLetterRecord;
import com.github.lisama569.localmessagequeuejava.mq.model.FailureKind;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 死信存储（broker 级，跨分区单个追加日志 {@code dead-letters.log}）。
 *
 * <p>每条死信保留：消息标识、主题、分区、位点、<b>原始载荷</b>、失败分类、
 * 失败原因、尝试次数、进入死信时间。追加即 fsync，重启后 {@link #loadAll()} 可恢复。
 * 末尾半行按「写入未完成」忽略。</p>
 */
public final class DeadLetterStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;
    private final Object writeLock = new Object();

    public DeadLetterStore(Path dataDir) {
        this.file = dataDir.resolve("dead-letters.log");
    }

    public DeadLetterStore open() throws IOException {
        Files.createDirectories(file.getParent());
        if (Files.notExists(file)) {
            Files.createFile(file);
        }
        return this;
    }

    public void append(DeadLetterRecord record) throws IOException {
        Map<String, Object> root = new LinkedHashMap<>();
        Message m = record.getOriginalMessage();
        root.put("id", m.getId());
        root.put("topic", m.getTopic());
        root.put("partition", m.getPartition());
        root.put("offset", m.getOffset());
        root.put("payload", m.getPayload());
        root.put("kind", record.getFailureKind().name());
        root.put("reason", record.getReason());
        root.put("attempts", record.getAttempts());
        root.put("deadLetteredAt", record.getDeadLetteredAt());
        byte[] line = MAPPER.writeValueAsBytes(root);
        synchronized (writeLock) {
            try (FileChannel channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                channel.write(java.nio.ByteBuffer.wrap(line));
                channel.write(java.nio.ByteBuffer.wrap(new byte[]{'\n'}));
                channel.force(true);
            }
        }
    }

    public List<DeadLetterRecord> loadAll() throws IOException {
        List<DeadLetterRecord> records = new ArrayList<>();
        if (Files.notExists(file)) {
            return records;
        }
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> root = MAPPER.readValue(line, Map.class);
                    Message message = new Message(
                            (String) root.get("id"),
                            (String) root.get("topic"),
                            ((Number) root.get("partition")).intValue(),
                            ((Number) root.get("offset")).longValue(),
                            (String) root.get("payload"));
                    records.add(new DeadLetterRecord(
                            message,
                            FailureKind.valueOf((String) root.get("kind")),
                            (String) root.get("reason"),
                            ((Number) root.get("attempts")).intValue(),
                            ((Number) root.get("deadLetteredAt")).longValue()));
                } catch (RuntimeException parseError) {
                    break;
                }
            }
        }
        return records;
    }
}
