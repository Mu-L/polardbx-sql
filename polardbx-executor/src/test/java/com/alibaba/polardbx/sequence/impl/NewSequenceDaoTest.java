package com.alibaba.polardbx.sequence.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;

public class NewSequenceDaoTest {

    private NewSequenceDao dao;
    private Method needToRetryMethod;

    @Before
    public void setUp() throws Exception {
        dao = new NewSequenceDao();
        needToRetryMethod = NewSequenceDao.class.getDeclaredMethod(
            "needToRetryForNewConnection", Exception.class, long.class);
        needToRetryMethod.setAccessible(true);
    }

    private boolean invokeNeedToRetry(Exception e, long startRequestTime) throws Exception {
        return (boolean) needToRetryMethod.invoke(dao, e, startRequestTime);
    }

    private long withinTimeout() {
        return System.currentTimeMillis();
    }

    private long expiredTimeout() {
        return System.currentTimeMillis() - NewSequenceDao.TOTAL_REQUEST_TIMEOUT - 1000;
    }

    @Test
    public void testRetryOnChannelInactive() throws Exception {
        Assert.assertTrue(invokeNeedToRetry(new Exception("channel inactive"), withinTimeout()));
    }

    @Test
    public void testRetryOnPreviousUnfinished() throws Exception {
        Assert.assertTrue(invokeNeedToRetry(new Exception("previous unfinished request"), withinTimeout()));
    }

    @Test
    public void testRetryOnTimeout() throws Exception {
        Assert.assertTrue(invokeNeedToRetry(new Exception("connection timeout"), withinTimeout()));
    }

    @Test
    public void testRetryOnClientRemoved() throws Exception {
        Assert.assertTrue(invokeNeedToRetry(new Exception("Client removed"), withinTimeout()));
    }

    @Test
    public void testRetryOnClosed() throws Exception {
        Assert.assertTrue(invokeNeedToRetry(new Exception("connection closed"), withinTimeout()));
    }

    @Test
    public void testRetryOnEOF() throws Exception {
        Assert.assertTrue(invokeNeedToRetry(new Exception("unexpected EOF"), withinTimeout()));
    }

    @Test
    public void testRetryOnQueryInterrupted() throws Exception {
        Assert.assertTrue(invokeNeedToRetry(
            new Exception("Query execution was interrupted"), withinTimeout()));
    }

    @Test
    public void testRetryOnNotAllowedDuringSwichover() throws Exception {
        Assert.assertTrue(invokeNeedToRetry(
            new Exception("The server is not allowed to do current operation"), withinTimeout()));
    }

    @Test
    public void testNoRetryOnUnknownError() throws Exception {
        Assert.assertFalse(invokeNeedToRetry(new Exception("some unknown error"), withinTimeout()));
    }

    @Test
    public void testNoRetryOnNullMessage() throws Exception {
        Assert.assertFalse(invokeNeedToRetry(new Exception((String) null), withinTimeout()));
    }

    @Test
    public void testNoRetryWhenTimeoutExpired() throws Exception {
        Assert.assertFalse(invokeNeedToRetry(new Exception("channel inactive"), expiredTimeout()));
    }

    @Test
    public void testNoRetryOnUnknownErrorEvenExpired() throws Exception {
        Assert.assertFalse(invokeNeedToRetry(new Exception("some unknown error"), expiredTimeout()));
    }

    @Test
    public void testNoRetryOnNotAllowedWhenExpired() throws Exception {
        Assert.assertFalse(invokeNeedToRetry(
            new Exception("The server is not allowed to do current operation"), expiredTimeout()));
    }
}
