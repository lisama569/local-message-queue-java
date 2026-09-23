package com.github.lisama569.localmessagequeuejava.tests;

import com.github.lisama569.localmessagequeuejava.tests.harness.CrashHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 进程硬中断（kill -9）恢复测试。
 *
 * <p>通过 {@link ProcessBuilder} 启动独立 JVM 运行 {@link CrashHarness}，在消息处于
 * 「首次失败、待重试持久化」窗口用 {@code destroyForcibly}（SIGKILL）杀死进程——
 * 不执行任何 shutdown 钩子。随后用同一数据目录启动第二个进程接管，断言：</p>
 * <ul>
 *   <li>已确认消息（done-1）重启后不产生第二次副作用；</li>
 *   <li>崩溃时未确认的消息（mid-1）与排队消息（later-1）重启后被继续处理，不丢失；</li>
 *   <li>所有消息副作用各恰好一次。</li>
 * </ul>
 */
class CrashRecoveryTest {

    @TempDir
    Path dataDir;

    private static String classpathCache;

    private static synchronized String classpath() throws IOException, InterruptedException {
        if (classpathCache == null) {
            Path cp = Files.createTempFile("lmq-cp-", ".txt");
            Process p = new ProcessBuilder("mvn", "-q", "dependency:build-classpath",
                    "-Dmdep.outputFile=" + cp).redirectErrorStream(true).start();
            p.getInputStream().readAllBytes();
            int code = p.waitFor();
            String deps = code == 0 ? Files.readString(cp).trim() : "";
            classpathCache = "target/classes" + java.io.File.pathSeparator
                    + "target/test-classes"
                    + (deps.isEmpty() ? "" : java.io.File.pathSeparator + deps);
        }
        return classpathCache;
    }

    /** 子进程句柄：后台线程持续收集 stdout，命令与应答解耦。 */
    private static final class Harness implements AutoCloseable {
        private final Process process;
        private final BufferedWriter out;
        private final List<String> responses = new CopyOnWriteArrayList<>();
        private final java.util.concurrent.atomic.AtomicInteger pollSeq =
                new java.util.concurrent.atomic.AtomicInteger();
        private boolean hardKilled;

        Harness(String dir, int partitions, long backoffMs, int maxAttempts) throws Exception {
            process = new ProcessBuilder("java", "-cp", classpath(),
                    CrashHarness.class.getName()).redirectErrorStream(true).start();
            out = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(),
                    StandardCharsets.UTF_8));
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    process.getInputStream(), StandardCharsets.UTF_8));
            Thread pump = new Thread(() -> {
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        responses.add(line);
                        System.out.println("[harness] " + line);
                    }
                } catch (IOException ignored) {
                    // 进程被杀时流关闭
                }
            }, "harness-pump");
            pump.setDaemon(true);
            pump.start();
            cmd("dataDir=" + dir + " topic=crash partitions=" + partitions
                    + " backoffMs=" + backoffMs + " maxAttempts=" + maxAttempts);
            awaitResponse("HARNESS:started", 10_000);
        }

        void cmd(String command) throws IOException {
            synchronized (out) {
                out.write(command);
                out.newLine();
                out.flush();
            }
        }

        void send(int partition, String id, String payload) throws IOException {
            cmd("send\t" + partition + "\t" + id + "\t" + payload);
        }

        void failOnce(String id) throws IOException {
            cmd("failOnce " + id);
        }

        void awaitResponse(String prefix, long timeoutMs) throws IOException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                for (String r : responses) {
                    if (r.startsWith(prefix)) {
                        return;
                    }
                }
                sleepQuiet(10);
            }
            throw new IOException("等待子进程应答超时: " + prefix + "，已收到=" + responses);
        }

        void waitSideEffect(String id) throws IOException {
            awaitEffect(id, 1, 15_000);
        }

        /** 通过轮询 printState 等待某 id 的副作用计数达到期望值。 */
        void awaitEffect(String id, int expected, long timeoutMs) throws IOException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                int seq = pollSeq.incrementAndGet();
                String responsePrefix = "HARNESS:state-" + seq;
                synchronized (out) {
                    out.write("printState " + seq);
                    out.newLine();
                    out.flush();
                }
                String state = awaitRawResponse(responsePrefix, 2_000);
                if (extractEff(state, id) >= expected) {
                    return;
                }
                sleepQuiet(20);
            }
            throw new IOException("等待副作用超时: " + id + ">=" + expected);
        }

        void awaitInvocation(String id, int n, long timeoutMs) throws IOException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                int seq = pollSeq.incrementAndGet();
                synchronized (out) {
                    out.write("printState " + seq);
                    out.newLine();
                    out.flush();
                }
                String state = awaitRawResponse("HARNESS:state-" + seq, 2_000);
                if (extractInv(state, id) >= n) {
                    return;
                }
                sleepQuiet(20);
            }
            throw new IOException("等待调用次数超时: " + id + ">=" + n);
        }

        private String awaitRawResponse(String prefix, long timeoutMs) throws IOException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                for (String r : responses) {
                    if (r.startsWith(prefix)) {
                        return r;
                    }
                }
                sleepQuiet(10);
            }
            throw new IOException("应答超时: " + prefix);
        }

        private static int extractEff(String state, String id) {
            return extract(state, id, "eff=");
        }

        private static int extractInv(String state, String id) {
            return extract(state, id, "inv=");
        }

        private static int extract(String state, String id, String key) {
            String token = id + ":inv=";
            int i = state.indexOf(token);
            if (i < 0) {
                return 0;
            }
            int j = state.indexOf(key, i);
            if (j < 0) {
                return 0;
            }
            int k = j + key.length();
            int end = k;
            while (end < state.length() && Character.isDigit(state.charAt(end))) {
                end++;
            }
            return end == k ? 0 : Integer.parseInt(state.substring(k, end));
        }

        private static void sleepQuiet(long ms) throws IOException {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        void waitInvocation(String id, int n) throws IOException {
            awaitInvocation(id, n, 15_000);
        }

        /** 单次带唯一标记的状态查询。 */
        String state() throws IOException {
            int seq = pollSeq.incrementAndGet();
            String prefix = "HARNESS:state-final-" + seq;
            synchronized (out) {
                out.write("printState final-" + seq);
                out.newLine();
                out.flush();
            }
            return awaitRawResponse(prefix, 10_000);
        }

        void killHard() throws InterruptedException, IOException {
            process.toHandle().descendants().forEach(ProcessHandle::destroyForcibly);
            process.destroyForcibly();
            assertTrue(process.waitFor(10, TimeUnit.SECONDS), "hard kill should complete");
            assertTrue(!process.isAlive() && process.exitValue() != 0,
                    "SIGKILL 后进程应非正常退出");
            hardKilled = true;
        }

        @Override
        public void close() throws Exception {
            if (!hardKilled) {
                try {
                    cmd("exit");
                    process.waitFor(10, TimeUnit.SECONDS);
                } catch (IOException | RuntimeException ignored) {
                    // 进程可能已退出
                } finally {
                    process.destroyForcibly();
                }            }
        }
    }

    @Test
    void hardKillBeforeAck_redeliversWithoutLossOrDuplicate() throws Exception {
        String dir = dataDir.toString();

        try (Harness h1 = new Harness(dir, 1, 3000, 10)) {
            h1.failOnce("mid-1");
            h1.awaitResponse("HARNESS:armed-once mid-1", 5_000);
            h1.send(0, "done-1", "done-payload");
            h1.send(0, "mid-1", "mid-payload");
            h1.send(0, "later-1", "later-payload");
            h1.awaitResponse("HARNESS:sent later-1", 5_000);
            h1.waitSideEffect("done-1");
            // mid-1 第一次失败后，待重试态已 fsync，且退避3s窗口足够：立即 SIGKILL
            h1.waitInvocation("mid-1", 1);
            h1.killHard();
        }

        try (Harness h2 = new Harness(dir, 1, 3000, 10)) {
            h2.waitSideEffect("mid-1");
            h2.waitSideEffect("later-1");
            Thread.sleep(200);
            String state = h2.state();
            System.out.println("[判定依据] 重启后 " + state);
            // done-1 在全新进程里从未被调用（不出现），或出现且 inv=0,eff=0
            assertTrue(!state.contains("done-1:")
                            || state.matches("(?s).*done-1:inv=0,eff=0.*"),
                    "[判定依据] 硬崩溃后已确认消息不得重复产生副作用: " + state);
            assertTrue(state.matches("(?s).*mid-1:inv=\\d+,eff=1.*"),
                    "[判定依据] 崩溃时未确认消息重启后不丢失且副作用恰好1次: " + state);
            assertTrue(state.matches("(?s).*later-1:inv=\\d+,eff=1.*"),
                    "[判定依据] 崩溃时排队消息重启后不丢失且副作用恰好1次: " + state);
            System.out.println("[判定依据] kill -9 恢复：不丢失(mid/later eff=1)、"
                    + "不重复(done-1 inv=0)、状态来自磁盘账本/位点/待重试");
        }
    }
}
