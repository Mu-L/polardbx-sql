package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group2;

import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.TestClass;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.TestWithParameters;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GsiBackfillTypeTest 专用的参数化类内并发 Runner。
 * <p>
 * 背景: CI 的 surefire 固定按 suitesAndClasses 并发, 参数化用例在类内仍然串行,
 * 全类型矩阵(69 列 x 4 方法 = 276 用例)串行需 ~92 分钟。该测试类已完成用例间
 * 表级隔离(独立表名)与 DDL 超时/残留 job 清理防护, 因此可以在类内用固定线程池
 * 并发执行各参数实例, 不依赖 surefire 的 parallel 配置。
 * <p>
 * 线程安全边界: 仅限表级隔离的测试类使用, 套用到共享表名的测试会互相踩踏;
 * 并发度默认 8, 可用 -Dgsi.test.parallelism 覆盖。
 */
public class ParallelGsiRunner extends Suite {

    /**
     * 默认并发度: 8。 CI 为 2CN×(6c/16Gi), DDL 随连接分散到两个 CN,
     * 单 CN 承受的并行度约为 surefire 线程数×8的一半, 与 LOGICAL_DDL_PARALLELISM=8
     * 匹配; 本地单 CN 下 8 路会资源饱和, 可用 -Dgsi.test.parallelism 降低
     */
    private static final int DEFAULT_PARALLELISM = 8;

    public ParallelGsiRunner(Class<?> klass) throws Throwable {
        super(klass, createRunners(klass));
    }

    @Override
    public void run(RunNotifier notifier) {
        final List<Runner> children = getChildren();
        final int parallelism = Math.min(resolveParallelism(), children.size());
        if (parallelism <= 1) {
            super.run(notifier);
            return;
        }
        final ExecutorService pool = Executors.newFixedThreadPool(parallelism, newNamedThreadFactory());
        try {
            for (final Runner child : children) {
                pool.execute(() -> child.run(notifier));
            }
            // 等待全部参数实例结束; 单类全量最长数小时, 预留 1 天上限防御
            pool.shutdown();
            pool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            pool.shutdownNow();
        }
    }

    private static List<Runner> createRunners(Class<?> klass) throws Throwable {
        final TestClass testClass = new TestClass(klass);
        final FrameworkMethod parametersMethod = findParametersMethod(testClass);
        final Parameterized.Parameters parameters =
            parametersMethod.getAnnotation(Parameterized.Parameters.class);

        final Object value = parametersMethod.invokeExplosively(null);
        if (!(value instanceof Iterable)) {
            throw new IllegalStateException(
                parametersMethod.getName() + " must return an Iterable of arrays.");
        }

        final List<Runner> runners = new ArrayList<>();
        int index = 0;
        for (Object each : (Iterable<?>) value) {
            final Object[] params = each instanceof Object[] ? (Object[]) each : new Object[] {each};
            runners.add(new BlockJUnit4ClassRunnerWithParameters(
                createTestWithParameters(testClass, parameters.name(), index++, params)));
        }
        return runners;
    }

    private static FrameworkMethod findParametersMethod(TestClass testClass) throws Exception {
        final List<FrameworkMethod> methods = testClass.getAnnotatedMethods(Parameterized.Parameters.class);
        for (FrameworkMethod each : methods) {
            if (each.isStatic() && each.isPublic()) {
                return each;
            }
        }
        throw new Exception("No public static parameters method on class " + testClass.getName());
    }

    private static TestWithParameters createTestWithParameters(
        TestClass testClass, String pattern, int index, Object[] parameters) {
        final String finalPattern = pattern.replaceAll("\\{index\\}", Integer.toString(index));
        final String name = MessageFormat.format(finalPattern, parameters);
        return new TestWithParameters("[" + name + "]", testClass, Arrays.asList(parameters));
    }

    private static int resolveParallelism() {
        final String value = System.getProperty("gsi.test.parallelism", "").trim();
        if (value.isEmpty()) {
            return DEFAULT_PARALLELISM;
        }
        try {
            return Math.max(1, Integer.parseInt(value));
        } catch (NumberFormatException ignore) {
            return DEFAULT_PARALLELISM;
        }
    }

    private static ThreadFactory newNamedThreadFactory() {
        final AtomicInteger seq = new AtomicInteger();
        return r -> {
            final Thread thread = new Thread(r, "gsi-parallel-runner-" + seq.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
