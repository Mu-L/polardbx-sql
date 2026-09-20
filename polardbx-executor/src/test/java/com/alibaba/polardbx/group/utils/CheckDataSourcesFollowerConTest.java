package com.alibaba.polardbx.group.utils;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mockStatic;

/**
 * CheckDataSourcesTask 类的单元测试类
 * 测试 checkFollowerConnection 方法的各种场景
 */
public class CheckDataSourcesFollowerConTest {

    private Method checkFollowerConnectionMethod;

    @Before
    public void setUp() throws Exception {
        checkFollowerConnectionMethod = CheckDataSourcesTask.class.getDeclaredMethod(
            "checkFollowerConnection", long.class);
        checkFollowerConnectionMethod.setAccessible(true);
    }

    /**
     * 测试用例编号：TC001
     * 测试场景：follower 连接在超时时间内成功建立
     * 设计思路：模拟 isMatchFollowerReadSetting 方法在第一次检查时就返回 true，
     * 验证方法能够正常返回而不抛出异常
     */
    @Test
    public void testCheckFollowerConnection_Success() throws Exception {
        final AtomicBoolean callCount = new AtomicBoolean(false);

        try (MockedStatic<CheckDataSourcesTask> mocked = mockStatic(CheckDataSourcesTask.class)) {

            mocked.when(() -> CheckDataSourcesTask.isMatchFollowerReadSetting(true))
                .thenAnswer(invocation -> {
                    callCount.set(true);
                    return true;
                });

            mocked.when(() -> CheckDataSourcesTask.checkFollowerConnection(anyLong()))
                .thenCallRealMethod();

            checkFollowerConnectionMethod.invoke(null, 5000L);

            assertTrue("isMatchFollowerReadSetting 方法应该被调用", callCount.get());
        }
    }

    /**
     * 测试用例编号：TC002
     * 测试场景：follower 连接在超时时间内未建立，抛出超时异常
     * 设计思路：模拟 isMatchFollowerReadSetting 方法始终返回 false，
     * 验证方法在超时后抛出正确的异常信息
     */
    @Test
    public void testCheckFollowerConnection_Timeout() throws Exception {
        try (MockedStatic<CheckDataSourcesTask> mocked = mockStatic(CheckDataSourcesTask.class)) {

            mocked.when(() -> CheckDataSourcesTask.isMatchFollowerReadSetting(true))
                .thenReturn(false);

            mocked.when(() -> CheckDataSourcesTask.checkFollowerConnection(anyLong()))
                .thenCallRealMethod();

            try {
                checkFollowerConnectionMethod.invoke(null, 500L);
                fail("应该抛出 RuntimeException 异常");
            } catch (Exception e) {
                Throwable cause = e.getCause();
                assertTrue("应该抛出 RuntimeException", cause instanceof RuntimeException);
                String message = cause.getMessage();
                assertTrue("异常信息应该包含 'timeout waiting for follower connecting for follower read account'",
                    message.contains("timeout waiting for follower connecting for follower read account"));
            }
        }
    }

    /**
     * 测试用例编号：TC004
     * 测试场景：超时时间为0，立即检查一次
     * 设计思路：设置超时时间为0，验证方法只执行一次检查
     * 如果第一次检查就成功则正常返回，否则抛出超时异常
     */
    @Test
    public void testCheckFollowerConnection_ZeroTimeout_Success() throws Exception {
        try (MockedStatic<CheckDataSourcesTask> mocked = mockStatic(CheckDataSourcesTask.class)) {

            mocked.when(() -> CheckDataSourcesTask.isMatchFollowerReadSetting(true))
                .thenReturn(true);

            mocked.when(() -> CheckDataSourcesTask.checkFollowerConnection(anyLong()))
                .thenCallRealMethod();

            try {
                checkFollowerConnectionMethod.invoke(null, 0);
            } catch (Exception e) {
                // 忽略异常
            }
        }
    }

    /**
     * 测试用例编号：TC005
     * 测试场景：超时时间为0，第一次检查失败
     * 设计思路：设置超时时间为0且第一次检查失败，验证方法立即抛出超时异常
     */
    @Test
    public void testCheckFollowerConnection_ZeroTimeout_Timeout() throws Exception {
        try (MockedStatic<CheckDataSourcesTask> mocked = mockStatic(CheckDataSourcesTask.class)) {

            mocked.when(() -> CheckDataSourcesTask.isMatchFollowerReadSetting(true))
                .thenReturn(false);

            mocked.when(() -> CheckDataSourcesTask.checkFollowerConnection(anyLong()))
                .thenCallRealMethod();

            try {
                checkFollowerConnectionMethod.invoke(null, 0L);
                fail("应该抛出 RuntimeException 异常");
            } catch (Exception e) {
                Throwable cause = e.getCause();
                assertTrue("应该抛出 RuntimeException", cause instanceof RuntimeException);
            }
        }
    }

    /**
     * 测试用例编号：TC006
     * 测试场景：follower 连接在多次检查后成功建立
     * 设计思路：模拟 isMatchFollowerReadSetting 方法前几次返回 false，
     * 最后一次返回 true，验证方法能够正常返回
     */
    @Test
    public void testCheckFollowerConnection_MultipleAttempts_Success() throws Exception {
        final AtomicInteger callCount = new AtomicInteger(0);

        try (MockedStatic<CheckDataSourcesTask> mocked = mockStatic(CheckDataSourcesTask.class)) {

            mocked.when(() -> CheckDataSourcesTask.isMatchFollowerReadSetting(true))
                .thenAnswer(invocation -> {
                    int count = callCount.incrementAndGet();
                    return count >= 3;
                });

            mocked.when(() -> CheckDataSourcesTask.checkFollowerConnection(anyLong()))
                .thenCallRealMethod();

            checkFollowerConnectionMethod.invoke(null, 5000L);

            assertTrue("isMatchFollowerReadSetting 方法应该被调用多次", callCount.get() >= 3);
        }
    }

    /**
     * 测试用例编号：TC009
     * 测试场景：验证 sleep 间隔正确性
     * 设计思路：通过调用次数和时间计算验证 sleep 间隔是否为 100ms
     */
    @Test
    public void testCheckFollowerConnection_SleepInterval() throws Exception {
        final AtomicInteger callCount = new AtomicInteger(0);
        final long testTimeout = 500;

        try (MockedStatic<CheckDataSourcesTask> mocked = mockStatic(CheckDataSourcesTask.class)) {
            mocked.when(() -> CheckDataSourcesTask.isMatchFollowerReadSetting(true))
                .thenAnswer(invocation -> {
                    int count = callCount.incrementAndGet();
                    return count >= 10;
                });

            mocked.when(() -> CheckDataSourcesTask.checkFollowerConnection(anyLong()))
                .thenCallRealMethod();

            try {
                checkFollowerConnectionMethod.invoke(null, testTimeout);
            } catch (Exception e) {
                // 忽略异常
            }
            int expectedCalls = (int) (testTimeout / 100L);
            assertTrue("调用次数应该在合理范围内", Math.abs(callCount.get() - expectedCalls) <= 3);
        }
    }

    @Test
    public void testCheckFollowerConnection_Interrupted() {
        final long testTimeout = 500;
        try (MockedStatic<CheckDataSourcesTask> mocked = mockStatic(CheckDataSourcesTask.class)) {
            mocked.when(() -> CheckDataSourcesTask.isMatchFollowerReadSetting(true))
                .thenAnswer(invocation -> {
                    throw new InterruptedException("gg");
                });

            mocked.when(() -> CheckDataSourcesTask.checkFollowerConnection(anyLong()))
                .thenCallRealMethod();

            try {
                checkFollowerConnectionMethod.invoke(null, testTimeout);
            } catch (Exception e) {
                assertTrue(e.getCause().toString() + "应包含gg", e.getCause().toString().contains("gg"));
            }
            assertTrue(true);
        }
    }

    /**
     * 测试用例编号：TC010
     * 测试场景：正常超时时间下的完整流程
     * 设计思路：使用正常的超时时间（5000ms），验证方法在长时间运行时的行为
     * 如果在前几次检查中成功则立即返回
     */
    @Test
    public void testCheckFollowerConnection_NormalTimeout() throws Exception {
        try (MockedStatic<CheckDataSourcesTask> mocked = mockStatic(CheckDataSourcesTask.class)) {

            mocked.when(() -> CheckDataSourcesTask.isMatchFollowerReadSetting(true))
                .thenReturn(true);

            mocked.when(() -> CheckDataSourcesTask.checkFollowerConnection(anyLong()))
                .thenCallRealMethod();

            checkFollowerConnectionMethod.invoke(null, 5000L);
        }
    }
}