package com.alibaba.polardbx.executor.gsi;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.ArrayList;
import java.util.List;

public class BackfillParameterManager {
    public static class BackfillConcurrencyParameter {
        public long getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(long batchSize) {
            this.batchSize = batchSize;
        }

        public long getSpeedLimit() {
            return speedLimit;
        }

        public void setSpeedLimit(long speedLimit) {
            this.speedLimit = speedLimit;
        }

        public long getSpeedMin() {
            return speedMin;
        }

        public void setSpeedMin(long speedMin) {
            this.speedMin = speedMin;
        }

        public long getParallelism() {
            return parallelism;
        }

        public void setParallelism(long parallelism) {
            this.parallelism = parallelism;
        }

        public BackfillConcurrencyParameter(long batchSize, long speedLimit, long speedMin, long parallelism) {
            this.batchSize = batchSize;
            this.speedLimit = speedLimit;
            this.speedMin = speedMin;
            this.parallelism = parallelism;
        }

        long batchSize;
        long speedLimit;
        long speedMin;
        long parallelism;

        public List<String> showParameter() {
            List<String> result = new ArrayList<>();
            String perfStr = String.format("    PARAMETER[BATCH_SIZE=%d, SPEED_LIMIT=%d, SPEED_MIN=%d, PARALLELISM=%d]",
                batchSize, speedLimit, speedMin, parallelism);
            result.add(perfStr);
            return result;
        }

    }

    public static BackfillConcurrencyParameter newBackfillParameter(ExecutionContext ec, String schemaName,
                                                                    String primaryTable) {
        if (DdlHelper.isBoostPerfMode(ec)) {
            return newBackfillParameterInBoostModeWithExecutionContext(ec, schemaName, primaryTable);
        } else {
            return newBackfillParameterWithExecutionContext(ec, schemaName, primaryTable);
        }
    }

    public static BackfillConcurrencyParameter newBackfillParameterInBoostModeWithExecutionContext(ExecutionContext ec,
                                                                                                   String schemaName,
                                                                                                   String primaryTable) {
        long tableAvgRowLength = Extractor.getLogicalTableAvgRowLength(schemaName, primaryTable);
        Long batchSize = calBatchSize(ec, schemaName, tableAvgRowLength);
        Long speedLimit = calMaxSpeed(ec, schemaName, tableAvgRowLength);
        Long parallelism = (long) (ThreadCpuStatUtil.NUM_CORES * 2);
        // we set a big value at very beginning.
        Long speedMin = speedLimit;
        return new BackfillConcurrencyParameter(batchSize, speedLimit, speedMin, parallelism);
    }

    public static BackfillConcurrencyParameter newBackfillParameterWithExecutionContext(ExecutionContext ec,
                                                                                        String schemaName,
                                                                                        String primaryTable) {
        ParamManager pm = ec.getParamManager();
        Long batchSize = pm.getLong(ConnectionParams.GSI_BACKFILL_BATCH_SIZE);
        Long speedLimit = pm.getLong(ConnectionParams.GSI_BACKFILL_SPEED_LIMITATION);
        Long parallelism = pm.getLong(ConnectionParams.GSI_BACKFILL_PARALLELISM);
        Long speedMin = pm.getLong(ConnectionParams.GSI_BACKFILL_SPEED_MIN);
        return new BackfillConcurrencyParameter(batchSize, speedLimit, speedMin, parallelism);
    }

    private static Long calMaxSpeed(ExecutionContext ec, String schemaName, long tableAvgRowLength) {
        ParamManager pm = ec.getParamManager();
        long maxbatchFileSize = pm.getLong(ConnectionParams.MAX_BATCH_FILE_SIZE_SPEED);
        long idealBatchSize = maxbatchFileSize / tableAvgRowLength;
        return idealBatchSize;
    }

    private static Long calBatchSize(ExecutionContext ec, String schemaName, long tableAvgRowLength) {
        ParamManager pm = ec.getParamManager();
        long batchFileSize = pm.getLong(ConnectionParams.BATCH_FILE_SIZE);
        long idealBatchSize = batchFileSize / tableAvgRowLength;
        long actualBatchSize = 1;
        if (idealBatchSize <= 0) {
            actualBatchSize = 1;
        } else {
            actualBatchSize = idealBatchSize;
        }
        return actualBatchSize;
    }
}
