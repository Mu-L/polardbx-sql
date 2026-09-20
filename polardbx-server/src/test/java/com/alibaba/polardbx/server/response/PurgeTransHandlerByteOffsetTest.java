package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.parser.ServerParse;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Reproduces aone#85021553: {@link ServerParse#parse(ByteString)} computes the
 * PURGE TRANS parameter offset on the byte-oriented {@link ByteString}, but
 * {@link PurgeTransHandler} consumes that offset on a char-oriented {@link String}
 * (as produced by {@code sql.toString()} in ServerQueryHandler). When the SQL prefix
 * contains multi-byte characters (e.g. a Chinese comment), the byte offset no longer
 * matches the char index, causing either an out-of-bounds exception on short
 * parameters or a silently truncated parameter value on long parameters.
 */
public class PurgeTransHandlerByteOffsetTest {

    private static int byteOffsetOf(ByteString sql) {
        int rs = ServerParse.parse(sql);
        assertEquals("test SQL must be recognized as PURGE_TRANS", ServerParse.PURGE_TRANS, rs & 0xff);
        return rs >>> 8;
    }

    private static boolean invokeDoExecute(PurgeTransHandler handler) throws Exception {
        Method doExecute = AbstractTransHandler.class.getDeclaredMethod("doExecute");
        doExecute.setAccessible(true);
        return (boolean) doExecute.invoke(handler);
    }

    /**
     * H1: short parameter scenario. "PURGE TRANS BEFORE 7" prefixed by a Chinese
     * comment. The byte offset computed by ServerParse should let PurgeTransHandler
     * correctly parse "before" as 7, exactly as consuming the same offset against the
     * original ByteString would. The current buggy implementation instead throws
     * StringIndexOutOfBoundsException because it consumes the byte offset on a
     * char-indexed String (sql.toString()), so this test is expected to fail until
     * the fix makes offset-consumption byte-accurate.
     */
    @Test
    public void testChineseCommentPrefix_ShortParam_ByteOffsetOutOfBoundsOnCharString() throws Exception {
        String rawSql = "/* 中文注释 */PURGE TRANS BEFORE 7";
        ByteString sql = ByteString.from(rawSql);
        int byteOffset = byteOffsetOf(sql);

        // Sanity: byte offset and char offset diverge because of the multi-byte comment.
        assertNotEquals("byte offset and char offset must diverge for this multi-byte prefix",
            byteOffset, rawSql.indexOf('7'));

        // Correct behavior (consuming on ByteString) yields "7".
        assertEquals("7", sql.substring(byteOffset));

        ServerConnection c = mock(ServerConnection.class);
        stubPacketWriting(c);
        PurgeTransHandler handler = new PurgeTransHandler(sql.toString(), byteOffset, c);

        TransactionManager transactionManager = mock(TransactionManager.class);
        TransactionExecutor executor = mock(TransactionExecutor.class);
        AsyncTaskQueue asyncQueue = mock(AsyncTaskQueue.class);
        when(transactionManager.getTransactionExecutor()).thenReturn(executor);
        when(executor.getAsyncQueue()).thenReturn(asyncQueue);
        setTransactionManager(handler, transactionManager);

        // Capture the real "beforeSeconds" value that doExecute() computed internally
        // via Integer.parseInt(str) and forwarded into RotateGlobalTxLogTask's constructor.
        final int[] capturedBeforeSeconds = {Integer.MIN_VALUE};
        boolean executeResult;
        try (org.mockito.MockedConstruction<com.alibaba.polardbx.transaction.async.RotateGlobalTxLogTask>
            rotateTaskMock = org.mockito.Mockito.mockConstruction(
            com.alibaba.polardbx.transaction.async.RotateGlobalTxLogTask.class,
            (mock, context) -> capturedBeforeSeconds[0] = (Integer) context.arguments().get(3));
            org.mockito.MockedStatic<com.alibaba.polardbx.executor.scheduler.executor.trx.CleanLogTableTask>
                cleanLogMock = org.mockito.Mockito.mockStatic(
                com.alibaba.polardbx.executor.scheduler.executor.trx.CleanLogTableTask.class)) {
            cleanLogMock.when(() -> com.alibaba.polardbx.executor.scheduler.executor.trx.CleanLogTableTask
                .run(org.mockito.ArgumentMatchers.anyBoolean(), org.mockito.ArgumentMatchers.any())).thenReturn(0L);

            executeResult = invokeDoExecute(handler);
        }

        assertEquals(
            "PurgeTransHandler must consume the byte offset accurately and parse 'before' as 7, "
                + "exactly as consuming it against the original ByteString would",
            7, capturedBeforeSeconds[0]);
        assertEquals("doExecute() must succeed once the parameter is parsed correctly", true, executeResult);
    }

    /**
     * H2: long parameter scenario. "PURGE TRANS BEFORE 123456789" prefixed by a
     * Chinese comment. The byte offset is small enough not to overflow the char
     * String, but it lands in the middle of the digits, so the buggy String-based
     * substring silently truncates the parameter instead of returning "123456789".
     */
    @Test
    public void testChineseCommentPrefix_LongParam_ByteOffsetSilentlyTruncatesOnCharString() throws Exception {
        String rawSql = "/* 中文注释 */PURGE TRANS BEFORE 123456789";
        ByteString sql = ByteString.from(rawSql);
        int byteOffset = byteOffsetOf(sql);

        // Correct behavior (consuming on ByteString) yields the full parameter.
        String expected = sql.substring(byteOffset);
        assertEquals("123456789", expected);

        ServerConnection c = mock(ServerConnection.class);
        stubPacketWriting(c);
        PurgeTransHandler handler = new PurgeTransHandler(sql.toString(), byteOffset, c);

        // Wire a mocked TransactionManager so doExecute() can proceed past the
        // parameter-parsing block without NPEs, regardless of whether it is truncated.
        TransactionManager transactionManager = mock(TransactionManager.class);
        TransactionExecutor executor = mock(TransactionExecutor.class);
        AsyncTaskQueue asyncQueue = mock(AsyncTaskQueue.class);
        when(transactionManager.getTransactionExecutor()).thenReturn(executor);
        when(executor.getAsyncQueue()).thenReturn(asyncQueue);
        setTransactionManager(handler, transactionManager);

        // Capture the real "beforeSeconds" value that doExecute() computed internally
        // via Integer.parseInt(str) and forwarded into RotateGlobalTxLogTask's constructor.
        final int[] capturedBeforeSeconds = {Integer.MIN_VALUE};
        try (org.mockito.MockedConstruction<com.alibaba.polardbx.transaction.async.RotateGlobalTxLogTask>
            rotateTaskMock = org.mockito.Mockito.mockConstruction(
            com.alibaba.polardbx.transaction.async.RotateGlobalTxLogTask.class,
            (mock, context) -> capturedBeforeSeconds[0] = (Integer) context.arguments().get(3));
            org.mockito.MockedStatic<com.alibaba.polardbx.executor.scheduler.executor.trx.CleanLogTableTask>
                cleanLogMock = org.mockito.Mockito.mockStatic(
                com.alibaba.polardbx.executor.scheduler.executor.trx.CleanLogTableTask.class)) {
            cleanLogMock.when(() -> com.alibaba.polardbx.executor.scheduler.executor.trx.CleanLogTableTask
                .run(org.mockito.ArgumentMatchers.anyBoolean(), org.mockito.ArgumentMatchers.any())).thenReturn(0L);

            invokeDoExecute(handler);
        }

        // Correct behavior: PurgeTransHandler must consume the byte offset accurately and
        // parse the full parameter value (123456789), exactly as consuming it against the
        // original ByteString would. The current buggy implementation silently truncates it.
        assertEquals(
            "PurgeTransHandler must consume the byte offset accurately; "
                + "consuming it on a char-indexed String silently truncated the parameter",
            Integer.parseInt(expected), capturedBeforeSeconds[0]);
    }

    private static void stubPacketWriting(ServerConnection c) {
        java.nio.ByteBuffer byteBuffer = java.nio.ByteBuffer.allocateDirect(4096);
        when(c.allocate()).thenReturn(new com.alibaba.polardbx.net.buffer.ByteBufferHolder(byteBuffer));
        when(c.checkWriteBuffer(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyInt()))
            .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
        org.mockito.Mockito.doCallRealMethod().when(c).writeToBuffer(org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any());
        when(c.writeToBuffer(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.any()))
            .thenAnswer(invocationOnMock -> {
                byte[] src = invocationOnMock.getArgument(0);
                int srcOffset = invocationOnMock.getArgument(1);
                int length = invocationOnMock.getArgument(2);
                com.alibaba.polardbx.net.buffer.ByteBufferHolder bufferHolder = invocationOnMock.getArgument(3);
                bufferHolder.put(src, srcOffset, length);
                return bufferHolder;
            });
    }

    private static void setTransactionManager(PurgeTransHandler handler, TransactionManager tm) throws Exception {
        java.lang.reflect.Field field = AbstractTransHandler.class.getDeclaredField("transactionManager");
        field.setAccessible(true);
        field.set(handler, tm);
    }
}
