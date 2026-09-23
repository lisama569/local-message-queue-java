package com.github.lisama569.localmessagequeuejava.mq.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.localmessagequeuejava.mq.model.FailureKind;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 分区状态存储。
 *
 * <p>磁盘上每个分区两个文件：</p>
 * <ul>
 *   <li>{@code *.ledger}：追加式去重账本（JSON Lines，
 *       {@code {"type":"COMPLETED"|"DEAD_LETTERED","id":...,"offset":...}}），
 *       只追加并 fsync。重启重放后得到「已完成消息」与「已死信消息」两个集合，
 *       重复投递（含位点回退、死信重放）都以账本为准跳过副作用；</li>
 *   <li>{@code *.state}：可变状态（已提交位点 + 待重试消息），通过
 *       「写临时文件 → fsync → 原子 rename」整体替换，任何时刻磁盘上要么是旧状态、
 *       要么是新状态，不会出现半个文件。</li>
 * </ul>
 *
 * <p>崩溃恢复边界：业务副作用与完成账本的落盘不是一个原子事务。框架在
 * <b>业务处理成功后</b> 先写账本再提交位点；若进程恰好在「副作用完成、账本落盘前」
 * 中断，该消息重启后会再处理一次（at-least-once 语义下的窄重复窗口），
 * 除此之外重复投递均由账本拦截。详见 README「交付语义」一节。</p>
 */
public final class PartitionStateStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path ledgerFile;
    private final Path stateFile;
    private final Path stateTmpFile;
    private final String topic;
    private final int partition;
    private final Object writeLock = new Object();

    private final Set<String> completed = new HashSet<>();
    private final Set<String> deadLettered = new HashSet<>();
    /** 已走完终态（成功/再次死信）的死信重放记录位点。 */
    private final Set<Long> replayedTerminalOffsets = new HashSet<>();
    private long committedOffset;
    private PendingRetry pendingRetry;

    /** 待重试消息的持久化状态。 */
    public record PendingRetry(long offset, int attempt, String reason, FailureKind kind, long readyAt) { }

    public PartitionStateStore(Path dir, String topic, int partition) {
        this.topic = Objects.requireNonNull(topic);
        this.partition = partition;
        String safeTopic = topic.replaceAll("[^A-Za-z0-9_.-]", "_");
        String prefix = safeTopic + "-" + partition;
        this.ledgerFile = dir.resolve(prefix + ".ledger");
        this.stateFile = dir.resolve(prefix + ".state");
        this.stateTmpFile = dir.resolve(prefix + ".state.tmp");
    }

    /** 加载账本与状态文件，重建内存视图。 */
    public PartitionStateStore open() throws IOException {
        Files.createDirectories(ledgerFile.getParent());
        loadLedger();
        loadState();
        return this;
    }

    private void loadLedger() throws IOException {
        if (Files.notExists(ledgerFile)) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(ledgerFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> record = MAPPER.readValue(line, Map.class);
                    String type = (String) record.get("type");
                    String id = (String) record.get("id");
                    if (type == null || id == null) {
                        break;
                    }
                    if ("COMPLETED".equals(type)) {
                        completed.add(id);
                    } else if ("DEAD_LETTERED".equals(type)) {
                        deadLettered.add(id);
                    }
                } catch (IOException parseError) {
                    // 末尾半行（崩溃）：忽略，保留此前完整前缀
                    break;
                }
            }
        }
    }

    private void loadState() throws IOException {
        if (Files.notExists(stateFile)) {
            this.committedOffset = 0;
            this.pendingRetry = null;
            this.replayedTerminalOffsets.clear();
            return;
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> root = MAPPER.readValue(Files.readAllBytes(stateFile), Map.class);
            Number offset = (Number) root.get("committedOffset");
            this.committedOffset = offset == null ? 0 : offset.longValue();
            Object pending = root.get("pendingRetry");
            if (pending instanceof Map<?, ?> p) {
                this.pendingRetry = new PendingRetry(
                        ((Number) p.get("offset")).longValue(),
                        ((Number) p.get("attempt")).intValue(),
                        (String) p.get("reason"),
                        FailureKind.valueOf((String) p.get("kind")),
                        ((Number) p.get("readyAt")).longValue());
            } else {
                this.pendingRetry = null;
            }
            Object replayOffsets = root.get("replayedTerminalOffsets");
            this.replayedTerminalOffsets.clear();
            if (replayOffsets instanceof Iterable<?> list) {
                for (Object o : list) {
                    if (o instanceof Number n) {
                        replayedTerminalOffsets.add(n.longValue());
                    }
                }
            }
        } catch (IOException | RuntimeException e) {
            // 状态文件理论上不会损坏（原子替换）；损坏时退回最保守状态：
            // 位点归零（账本仍保证不重复副作用），无待重试，无重放终态记录。
            this.committedOffset = 0;
            this.pendingRetry = null;
            this.replayedTerminalOffsets.clear();
        }
    }

    public long getCommittedOffset() {
        synchronized (writeLock) {
            return committedOffset;
        }
    }

    public boolean isCompleted(String messageId) {
        synchronized (writeLock) {
            return completed.contains(messageId);
        }
    }

    public boolean isDeadLettered(String messageId) {
        synchronized (writeLock) {
            return deadLettered.contains(messageId);
        }
    }

    /** 某条死信重放记录是否已走完终态（成功提交 / 再次死信）。 */
    public boolean isReplayTerminal(long offset) {
        synchronized (writeLock) {
            return replayedTerminalOffsets.contains(offset);
        }
    }

    /** 标记一条死信重放记录已走完终态。 */
    public void markReplayTerminal(long offset) throws IOException {
        synchronized (writeLock) {
            if (replayedTerminalOffsets.add(offset)) {
                persistState();
            }
        }
    }

    /** 记录一条「已完成」消息到追加账本并 fsync。 */
    public void recordCompleted(String messageId, long offset) throws IOException {
        Objects.requireNonNull(messageId);
        synchronized (writeLock) {
            appendLedger("COMPLETED", messageId, offset);
            completed.add(messageId);
        }
    }

    /** 记录一条「已死信」消息到追加账本并 fsync（重放不得绕过该约束）。 */
    public void recordDeadLettered(String messageId) throws IOException {
        Objects.requireNonNull(messageId);
        synchronized (writeLock) {
            appendLedger("DEAD_LETTERED", messageId, -1L);
            deadLettered.add(messageId);
        }
    }

    private void appendLedger(String type, String id, long offset) throws IOException {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("type", type);
        record.put("id", id);
        record.put("offset", offset);
        byte[] line = MAPPER.writeValueAsBytes(record);
        try (FileChannel channel = FileChannel.open(ledgerFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(java.nio.ByteBuffer.wrap(line));
            channel.write(java.nio.ByteBuffer.wrap(new byte[]{'\n'}));
            channel.force(true);
        }
    }

    public PendingRetry getPendingRetry() {
        synchronized (writeLock) {
            return pendingRetry;
        }
    }

    /**
     * 保存待重试状态（整体原子替换状态文件）。
     * 调用时机：每次判定为重试后、退避等待前——进程在退避期间被杀也能恢复。
     */
    public void savePendingRetry(long offset, int attempt, String reason,
                                 FailureKind kind, long readyAt) throws IOException {
        Objects.requireNonNull(kind);
        synchronized (writeLock) {
            this.pendingRetry = new PendingRetry(offset, attempt, reason, kind, readyAt);
            persistState();
        }
    }

    public void clearPendingRetry() throws IOException {
        synchronized (writeLock) {
            this.pendingRetry = null;
            persistState();
        }
    }

    /**
     * 提交位点（整体原子替换状态文件）。语义：offset 之前的消息均已终态处理。
     */
    public void commitOffset(long offset) throws IOException {
        synchronized (writeLock) {
            if (offset < committedOffset) {
                throw new IllegalArgumentException("commit offset cannot go backwards: "
                        + offset + " < " + committedOffset);
            }
            this.committedOffset = offset;
            persistState();
        }
    }

    private void persistState() throws IOException {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("topic", topic);
        root.put("partition", partition);
        root.put("committedOffset", committedOffset);
        if (pendingRetry != null) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("offset", pendingRetry.offset());
            p.put("attempt", pendingRetry.attempt());
            p.put("reason", pendingRetry.reason());
            p.put("kind", pendingRetry.kind().name());
            p.put("readyAt", pendingRetry.readyAt());
            root.put("pendingRetry", p);
        } else {
            root.put("pendingRetry", null);
        }
        root.put("replayedTerminalOffsets", new ArrayList<>(replayedTerminalOffsets));
        byte[] bytes = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
        Files.write(stateTmpFile, bytes,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        try (FileChannel channel = FileChannel.open(stateTmpFile, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        try {
            Files.move(stateTmpFile, stateFile, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException atomicNotSupported) {
            Files.move(stateTmpFile, stateFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
        try (FileChannel dirChannel = FileChannel.open(stateFile.getParent())) {
            dirChannel.force(true);
        } catch (IOException ignored) {
            // 某些平台不支持目录 fsync，rename 本身已是原子的
        }
    }
}
