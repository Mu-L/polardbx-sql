package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStat;
import com.alibaba.polardbx.optimizer.core.profiler.memory.MemoryEstimation;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import org.apache.calcite.rel.RelNode;

/**
 * @author fangwu
 */
public class MockRuntimeStat extends RuntimeStat {
    @Override
    public void collectThreadCpu(long threadCpuTimeCost) {

    }

    @Override
    public MemoryEstimation getMemoryEstimation() {
        return null;
    }

    @Override
    public CpuStat getCpuStat() {
        return null;
    }

    @Override
    public MemoryPool getMemoryPool() {
        return null;
    }

    @Override
    public RelNode getPlanTree() {
        return null;
    }

    @Override
    public void setPlanTree(RelNode rel) {

    }

    @Override
    public void addPhySqlCount(long phySqlCount) {

    }

    @Override
    public void addPhySqlTimecost(long phySqlTc) {

    }

    @Override
    public void addPhyFetchRows(long phyRsRows) {

    }

    @Override
    public void addPhyAffectedRows(long phyAffectiveRow) {

    }

    @Override
    public void addPhyConnTimecost(long phyConnTimecost) {

    }

    @Override
    public void addFetchTSOTimecost(long totalFetchTSOTimecost) {

    }

    @Override
    public void addFetchSequenceTimecost(long totalFetchSequenceTimecost) {

    }

    @Override
    public void setTrxType(String trxType) {

    }

    @Override
    public void addCommitPrepareTimecost(long commitPrepareTimecost) {

    }

    @Override
    public void addCommitLoggerTimecost(long commitLoggerTimecost) {

    }

    @Override
    public void addCommitTsoTimecost(long commitTsoTimecost) {

    }

    @Override
    public void addCommitCommitTimecost(long commitCommitTimecost) {

    }

    @Override
    public void addColumnarSnapshotTimecost(long columnarSnapshotTimecost) {

    }

    @Override
    public void setSqlType(SqlType sqlType) {

    }

    @Override
    public void setRunningWithCpuProfile(boolean runningWithCpuProfile) {

    }

    @Override
    public boolean isRunningWithCpuProfile() {
        return false;
    }

    @Override
    public void holdMemoryPool() {

    }
}
