package com.github.lisama569.localmessagequeuejava.tests;

import com.github.lisama569.localmessagequeuejava.mq.handler.MessageHandler;
import com.github.lisama569.localmessagequeuejava.mq.model.HandleResult;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** 测试公共设施：副作用计数处理器、临时数据目录。 */
final class MqTestSupport {

    private MqTestSupport() {
    }

    static Path tempDataDir(String prefix) throws IOException {
        return Files.createTempDirectory("lmq-" + prefix + "-");
    }

    /**
     * 可编排的处理器：
     * <ul>
     *   <li>{@code flakyFailUntil}：某 messageId 前几次调用返回 retry，之后成功（默认按消息计数）；</li>
     *   <li>{@code poison}：某 messageId 永远 retry（用于重试耗尽）；</li>
     *   <li>{@code nonRetryable}：某 messageId 首次即 deadLetter；</li>
     *   <li>{@code delayMs} / {@code slowIds}：模拟耗时（用于并发/超时）；</li>
     *   <li>{@code sideEffects}：按 messageId 统计真实业务副作用次数（关键断言：只出现一次）。</li>
     * </ul>
     */
    static final class ScriptedHandler implements MessageHandler {
        final Map<String, AtomicInteger> sideEffects = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> invocations = new ConcurrentHashMap<>();
        final List<String> orderedSideEffects = new CopyOnWriteArrayList<>();
        final Map<String, Integer> flakyFailUntil = new ConcurrentHashMap<>();
        final java.util.Set<String> poison = ConcurrentHashMap.newKeySet();
        final java.util.Set<String> nonRetryable = ConcurrentHashMap.newKeySet();
        final Map<String, Long> delays = new ConcurrentHashMap<>();
        long defaultDelayMs = 0L;
        /** 可选闸门：消息进入处理器时 countDown，用于等待消息真正在途。 */
        volatile java.util.concurrent.CountDownLatch gate;

        @Override
        public HandleResult handle(Message message) throws InterruptedException {
            String id = message.getId();
            int call = invocations.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            long delay = delays.getOrDefault(id, defaultDelayMs);
            if (gate != null) {
                gate.countDown();
            }
            if (delay > 0) {
                Thread.sleep(delay);
            }
            // 副作用：每条消息只应在最终成功时真正发生一次
            if (nonRetryable.contains(id)) {
                return HandleResult.deadLetter("poison: non-retryable by test script");
            }
            if (poison.contains(id)) {
                return HandleResult.retry("poison: always retryable-fail by test script call=" + call);
            }
            Integer failUntil = flakyFailUntil.get(id);
            if (failUntil != null && call <= failUntil) {
                return HandleResult.retry("transient failure call=" + call);
            }
            sideEffects.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            orderedSideEffects.add(id);
            return HandleResult.ack();
        }

        int sideEffectCount(String id) {
            AtomicInteger n = sideEffects.get(id);
            return n == null ? 0 : n.get();
        }

        int invocationCount(String id) {
            AtomicInteger n = invocations.get(id);
            return n == null ? 0 : n.get();
        }

        List<String> snapshotOrder() {
            return new ArrayList<>(orderedSideEffects);
        }
    }
}
