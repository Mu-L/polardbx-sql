package com.alibaba.polardbx.optimizer.core.planner.rule;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for AggregateGroupByDuplicatedRemoveRule thread safety.
 * <p>
 * AONE-80962996: AggregateGroupByDuplicatedRemoveRule 存在并发安全 Bug。
 * <p>
 * 该规则以 static final INSTANCE 单例注册，但 pattern 字段为实例变量（可变共享状态）。
 * 并发场景下多个线程同时执行 onMatch()，线程 B 将 pattern 重置为 null，
 * 线程 A 随后读到 null 触发 NPE。
 * <p>
 * 修复方案：将 pattern 从实例字段改为 onMatch() 内的局部变量。
 */
public class AggregateGroupByDuplicatedRemoveRuleTest {

    /**
     * 验证 AggregateGroupByDuplicatedRemoveRule 不应持有可变实例字段 pattern。
     * <p>
     * 该类以 static final INSTANCE 单例存在，所有线程共享同一实例。
     * 若存在可变实例字段 pattern，则并发调用 onMatch() 时必然存在数据竞争。
     * <p>
     * Red 阶段：当 pattern 仍是实例字段时，此测试失败。
     * Green 阶段：将 pattern 改为 onMatch() 内局部变量后，此测试通过。
     */
    @Test
    public void testPatternShouldNotBeInstanceField() throws Exception {
        Field[] fields = AggregateGroupByDuplicatedRemoveRule.class.getDeclaredFields();
        boolean hasMutablePatternInstanceField = false;
        for (Field f : fields) {
            if ("pattern".equals(f.getName()) && !Modifier.isStatic(f.getModifiers())) {
                hasMutablePatternInstanceField = true;
                break;
            }
        }
        assertFalse(
            "Thread safety bug (AONE-80962996): AggregateGroupByDuplicatedRemoveRule.INSTANCE "
                + "has a mutable instance field 'pattern'. Since INSTANCE is a static singleton "
                + "shared across threads, concurrent onMatch() calls race on this field: "
                + "Thread B resets pattern=null at line 46 while Thread A reads it at line 61 "
                + "causing NullPointerException. Fix: declare pattern as local variable in onMatch().",
            hasMutablePatternInstanceField
        );
    }

    /**
     * 验证单例实例不持有任何可变实例状态（无状态 = 线程安全）。
     * <p>
     * Red 阶段：pattern 字段存在时，可变实例字段数 > 0，断言失败。
     * Green 阶段：pattern 移为局部变量后，可变实例字段数为 0，断言通过。
     */
    @Test
    public void testSingletonShouldBeStateless() throws Exception {
        Field[] fields = AggregateGroupByDuplicatedRemoveRule.class.getDeclaredFields();
        int mutableInstanceFieldCount = 0;
        StringBuilder mutableFieldNames = new StringBuilder();
        for (Field f : fields) {
            int mods = f.getModifiers();
            // 排除 static 和 final 字段，统计可变实例字段
            if (!Modifier.isStatic(mods) && !Modifier.isFinal(mods)) {
                mutableInstanceFieldCount++;
                mutableFieldNames.append(f.getName()).append(" (").append(f.getType().getSimpleName()).append(") ");
            }
        }
        assertEquals(
            "AggregateGroupByDuplicatedRemoveRule INSTANCE should be completely stateless "
                + "(zero mutable instance fields) to be thread-safe. "
                + "Found " + mutableInstanceFieldCount + " mutable instance field(s): "
                + mutableFieldNames
                + "These cause race conditions when the singleton is accessed concurrently.",
            0,
            mutableInstanceFieldCount
        );
    }

    /**
     * 通过并发操作直接演示 pattern 字段的数据竞争问题。
     * <p>
     * 模拟 onMatch() 并发交错场景：
     * - 偶数线程：模拟线程 A 将 pattern 赋值为非空字符串（对应 onMatch 第 54 行）
     * - 奇数线程：模拟线程 B 将 pattern 重置为 null（对应 onMatch 第 46 行）
     * - 偶数线程随后读取 pattern，若为 null 则记录为检测到竞争
     * <p>
     * Red 阶段：字段存在时，可通过反射触发竞争，检测到 null-read 次数 > 0。
     * Green 阶段：字段不再存在，反射访问抛出 NoSuchFieldException，测试直接通过（跳过并发验证）。
     */
    @Test
    public void testConcurrentPatternRaceCondition() throws Exception {
        Field patternField;
        try {
            patternField = AggregateGroupByDuplicatedRemoveRule.class.getDeclaredField("pattern");
            patternField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            // Fix applied: pattern is now a local variable, field does not exist — test passes
            return;
        }

        final AggregateGroupByDuplicatedRemoveRule rule = AggregateGroupByDuplicatedRemoveRule.INSTANCE;
        final int threadCount = 16;
        final int iterationsPerThread = 5000;
        final AtomicInteger nullReadCount = new AtomicInteger(0);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int iter = 0; iter < iterationsPerThread; iter++) {
                            if (threadId % 2 == 0) {
                                // Thread A: set pattern to non-null (line 54 of onMatch)
                                patternField.set(rule, "-($0, ?");
                                // Thread A: later read pattern (line 61 of onMatch)
                                String p = (String) patternField.get(rule);
                                if (p == null) {
                                    // Thread B has reset pattern to null — this is the NPE scenario
                                    nullReadCount.incrementAndGet();
                                }
                            } else {
                                // Thread B: reset pattern to null (line 46 of onMatch)
                                patternField.set(rule, null);
                            }
                        }
                    } catch (Exception ignored) {
                        nullReadCount.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
            startLatch.countDown();
            doneLatch.await();
        } finally {
            executor.shutdown();
            patternField.set(rule, null);
        }

        assertEquals(
            "Concurrent race condition detected " + nullReadCount.get() + " time(s): "
                + "Thread B reset pattern=null while Thread A was reading it. "
                + "In real onMatch() this causes project.startsWith(null) → NullPointerException. "
                + "Fix: move 'pattern' from instance field to local variable inside onMatch().",
            0,
            nullReadCount.get()
        );
    }
}
