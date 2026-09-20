package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransactionManagerUtil;
import com.alibaba.polardbx.optimizer.utils.InventoryMode;
import com.alibaba.polardbx.stats.CurrentTransactionStatistics;
import com.alibaba.polardbx.stats.TransactionStatistics;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

public class MockTransaction implements ITransaction {

    @Override
    public long getId() {
        return 0;
    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public ExecutionContext getExecutionContext() {
        return null;
    }

    @Override
    public IConnectionHolder getConnectionHolder() {
        return null;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {

    }

    @Override
    public void tryClose() throws SQLException {

    }

    @Override
    public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw) throws SQLException {
        return null;
    }

    @Override
    public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw, ExecutionContext ec)
        throws SQLException {
        return null;
    }

    @Override
    public IConnection getConnection(String schemaName, String group, Long grpConnId, IDataSource ds, RW rw,
                                     ExecutionContext ec) throws SQLException {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public void setTraceId(String traceId) {

    }

    @Override
    public void updateStatisticsWhenStatementFinished(AtomicLong rowCount) {

    }

    @Override
    public void setMdlWaitTime(long mdlWaitTime) {

    }

    @Override
    public void setStartTimeInMs(long startTime) {

    }

    @Override
    public void setStartTime(long startTime) {

    }

    @Override
    public void setSqlStartTime(long sqlStartTime) {

    }

    @Override
    public void setSqlFinishTime(long t) {

    }

    @Override
    public void kill() throws SQLException {

    }

    @Override
    public void savepoint(String savepoint) {

    }

    @Override
    public void rollbackTo(String savepoint) {

    }

    @Override
    public void release(String savepoint) {

    }

    @Override
    public void clearTrxContext() {

    }

    @Override
    public void setCrucialError(ErrorCode errorCode, String cause) {

    }

    @Override
    public ErrorCode getCrucialError() {
        return null;
    }

    @Override
    public void checkCanContinue() {

    }

    @Override
    public boolean isDistributed() {
        return false;
    }

    @Override
    public boolean isDistributedWriteTrx() {
        return false;
    }

    @Override
    public State getState() {
        return null;
    }

    @Override
    public ITransactionPolicy.TransactionClass getTransactionClass() {
        return null;
    }

    @Override
    public long getStartTimeInMs() {
        return 0;
    }

    @Override
    public boolean isBegun() {
        return false;
    }

    @Override
    public InventoryMode getInventoryMode() {
        return ITransaction.super.getInventoryMode();
    }

    @Override
    public void setInventoryMode(InventoryMode inventoryMode) {

    }

    @Override
    public ITransactionManagerUtil getTransactionManagerUtil() {
        return null;
    }

    @Override
    public boolean handleStatementError(Throwable t, String traceId) {
        return false;
    }

    @Override
    public void releaseAutoSavepoint(String traceId) {

    }

    @Override
    public void releaseDirtyReadConnections() {

    }

    @Override
    public boolean isUnderCommitting() {
        return false;
    }

    @Override
    public boolean isAsyncCommit() {
        return false;
    }

    @Override
    public void updateCurrentStatistics(CurrentTransactionStatistics stat, long durationTimeMs) {
        ITransaction.super.updateCurrentStatistics(stat, durationTimeMs);
    }

    @Override
    public TransactionStatistics getStat() {
        return null;
    }

    @Override
    public TransactionType getType() {
        return null;
    }

    @Override
    public boolean isRwTransaction() {
        return ITransaction.super.isRwTransaction();
    }

    @Override
    public void setLastActiveTime() {

    }

    @Override
    public long getLastActiveTime() {
        return 0;
    }

    @Override
    public void resetLastActiveTime() {

    }

    @Override
    public long getIdleTimeout() {
        return 0;
    }

    @Override
    public long getIdleROTimeout() {
        return 0;
    }

    @Override
    public long getIdleRWTimeout() {
        return 0;
    }

    @Override
    public void clearFlashbackArea() {
        ITransaction.super.clearFlashbackArea();
    }

    @Override
    public String getUser() {
        return null;
    }
}