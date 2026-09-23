package com.github.lisama569.localmessagequeuejava.mq.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Objects;

/**
 * 单个分区的追加式消息日志。
 *
 * <p>磁盘格式为 JSON Lines，每行一条 {@code {"id":...,"payload":...,"replay":false}}，
 * 行号即位点（从 0 开始）。每次追加都 {@code flush + fsync}，进程中断后通过
 * {@link #loadAll()} 重放恢复；末尾不完整的行（崩溃时写了一半）会被忽略，
 * 不会破坏既有前缀——该行对应的 append 也从未对调用方返回成功。</p>
 */
public final class PartitionLog {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;
    private final String topic;
    private final int partition;
    private final Object writeLock = new Object();
    private long nextOffset;

    public PartitionLog(Path dir, String topic, int partition) {
        this.topic = Objects.requireNonNull(topic);
        this.partition = partition;
        String safeTopic = topic.replaceAll("[^A-Za-z0-9_.-]", "_");
        this.file = dir.resolve(safeTopic + "-" + partition + ".log");
    }

    /** 打开日志：必要时创建文件，并把末尾位点对齐到磁盘内容。 */
    public PartitionLog open() throws IOException {
        Files.createDirectories(file.getParent());
        if (Files.notExists(file)) {
            Files.createFile(file);
        }
        this.nextOffset = loadAll().size();
        return this;
    }

    /** 追加一条普通消息并持久化。 */
    public Message append(String id, String payload) throws IOException {
        return append(id, payload, false);
    }

    /**
     * 追加一条消息并持久化，返回带位点的不可变消息对象。
     *
     * @param replay true 表示死信重放投递（复用原消息标识，由去重账本拦截重复副作用）
     */
    public Message append(String id, String payload, boolean replay) throws IOException {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(payload, "payload");
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", id);
        record.put("payload", payload);
        record.put("replay", replay);
        byte[] line = MAPPER.writeValueAsBytes(record);
        synchronized (writeLock) {
            try (FileChannel channel = FileChannel.open(file,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                channel.write(java.nio.ByteBuffer.wrap(line));
                channel.write(java.nio.ByteBuffer.wrap(new byte[]{'\n'}));
                channel.force(true);
            }
            Message message = new Message(id, topic, partition, nextOffset, payload, replay);
            nextOffset++;
            return message;
        }
    }

    /**
     * 重放全部分区消息。最后一行若不是完整 JSON（崩溃写了一半）则忽略该行。
     */
    public List<Message> loadAll() throws IOException {
        if (Files.notExists(file)) {
            return List.of();
        }
        List<Message> messages = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> record = MAPPER.readValue(line, Map.class);
                    String id = (String) record.get("id");
                    String payload = (String) record.get("payload");
                    if (id == null || payload == null) {
                        break;
                    }
                    boolean replay = Boolean.TRUE.equals(record.get("replay"));
                    messages.add(new Message(id, topic, partition, messages.size(), payload, replay));
                } catch (IOException parseError) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new IOException("failed to read partition log: " + file, e);
        }
        return messages;
    }

    public long getLogEndOffset() {
        synchronized (writeLock) {
            return nextOffset;
        }
    }

    Path file() {
        return file;
    }
}
