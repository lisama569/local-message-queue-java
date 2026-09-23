package com.github.lisama569.localmessagequeuejava.tests.harness;

import com.github.lisama569.localmessagequeuejava.mq.LocalMessageBroker;
import com.github.lisama569.localmessagequeuejava.mq.model.BrokerConfig;
import com.github.lisama569.localmessagequeuejava.mq.model.HandleResult;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 进程级崩溃测试的被测进程入口。
 *
 * <p>协议（stdin 行命令，stdout 以 {@code HARNESS:} 前缀回执）：</p>
 * <pre>
 *   dataDir=&lt;dir&gt; topic=&lt;t&gt; partitions=&lt;n&gt; backoffMs=&lt;b&gt; maxAttempts=&lt;m&gt;
 *   send &lt;partition&gt; &lt;id&gt; &lt;payload&gt;
 *   failOnce &lt;id&gt;        该 id 首次处理返回 retry，之后成功
 *   failAlways &lt;id&gt;      该 id 永远 retry
 *   waitSideEffect &lt;id&gt;  阻塞直到某 id 产生副作用
 *   waitInvocation &lt;id&gt; &lt;n&gt;
 *   printState           输出已落盘状态行
 *   exit                 优雅退出
 * </pre>
 *
 * <p>父进程通过 {@code destroyForcibly}（SIGKILL）模拟 kill -9，不执行任何关停钩子。</p>
 */
public final class CrashHarness {

    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        LocalMessageBroker broker = null;
        Path dataDir = null;
        String topic = "crash";
        int partitions = 1;
        long backoffMs = 50;
        int maxAttempts = 10;
        java.util.Map<String, AtomicInteger> invocations = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Map<String, Integer> failUntil = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Set<String> failAlways = java.util.concurrent.ConcurrentHashMap.newKeySet();
        java.util.Map<String, AtomicInteger> effects = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.List<String> effectOrder = new java.util.concurrent.CopyOnWriteArrayList<>();

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            try {
                if (line.startsWith("dataDir=")) {
                    String[] kv = line.split("\\s+");
                    for (String entry : kv) {
                        if (entry.startsWith("dataDir=")) {
                            dataDir = Path.of(entry.substring("dataDir=".length()));
                        } else if (entry.startsWith("topic=")) {
                            topic = entry.substring("topic=".length());
                        } else if (entry.startsWith("partitions=")) {
                            partitions = Integer.parseInt(entry.substring("partitions=".length()));
                        } else if (entry.startsWith("backoffMs=")) {
                            backoffMs = Long.parseLong(entry.substring("backoffMs=".length()));
                        } else if (entry.startsWith("maxAttempts=")) {
                            maxAttempts = Integer.parseInt(entry.substring("maxAttempts=".length()));
                        }
                    }
                    BrokerConfig config = new BrokerConfig(Duration.ofMillis(backoffMs), maxAttempts,
                            1024, Duration.ofSeconds(5), Duration.ofSeconds(5));
                    broker = new LocalMessageBroker(dataDir, topic, partitions, config, message -> {
                        String id = message.getId();
                        int call = invocations.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
                        if (failAlways.contains(id)) {
                            return HandleResult.retry("failAlways call=" + call);
                        }
                        Integer until = failUntil.get(id);
                        if (until != null && call <= until) {
                            return HandleResult.retry("failOnce call=" + call);
                        }
                        effects.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
                        effectOrder.add(id);
                        return HandleResult.ack();
                    });
                    broker.start();
                    say("started");
                } else if (line.startsWith("send\t")) {
                    String[] p = line.split("\t", 4);
                    broker.sendToPartition(Integer.parseInt(p[1]), p[3], p[2]);
                    say("sent " + p[2]);
                } else if (line.startsWith("failOnce ")) {
                    String id = line.substring("failOnce ".length()).trim();
                    failUntil.merge(id, 1, Integer::sum);
                    say("armed-once " + id + " until=" + failUntil.get(id));
                } else if (line.startsWith("failAlways ")) {
                    failAlways.add(line.substring("failAlways ".length()).trim());
                    say("armed-always");
                } else if (line.startsWith("waitSideEffect ")) {
                    // 已废弃：调用方通过轮询 printState 等待，避免阻塞命令线程
                    say("obsolete");
                } else if (line.equals("printState") || line.startsWith("printState ")) {
                    // 支持唯一标记：printState <marker> => HARNESS:state-<marker> ...
                    String marker = line.length() > "printState".length()
                            ? line.substring("printState".length()).trim() : "";
                    java.util.Set<String> ids = new java.util.TreeSet<>();
                    ids.addAll(invocations.keySet());
                    ids.addAll(effects.keySet());
                    StringBuilder sb = new StringBuilder("state");
                    if (!marker.isEmpty()) {
                        sb.append('-').append(marker);
                    }
                    for (String id : ids) {
                        int inv = invocations.getOrDefault(id, new AtomicInteger()).get();
                        int eff = effects.getOrDefault(id, new AtomicInteger()).get();
                        sb.append(' ').append(id).append(":inv=").append(inv)
                                .append(",eff=").append(eff);
                    }
                    say(sb.toString());
                } else if (line.equals("exit")) {
                    say("bye");
                    broker.close();
                    return;
                } else {
                    say("UNKNOWN " + line);
                }
            } catch (Exception e) {
                say("ERROR " + e.getClass().getSimpleName() + " " + e.getMessage());
            }
        }
    }

    private static void say(String msg) {
        System.out.println("HARNESS:" + msg);
        System.out.flush();
    }
}
