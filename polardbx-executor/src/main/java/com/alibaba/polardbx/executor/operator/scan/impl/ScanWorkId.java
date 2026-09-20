package com.alibaba.polardbx.executor.operator.scan.impl;

import lombok.Data;

@Data
public class ScanWorkId {

    @Data
    public static class WorkId {
        private String queryId;
        private String logicalSchema;
        private String logicalTable;
        private String filePath;
        private int stripeId;
        private int workNumber;

        public WorkId(String queryId, String logicalSchema, String logicalTable, String filePath, int stripeId,
                      int workNumber) {
            this.queryId = queryId;
            this.logicalSchema = logicalSchema;
            this.logicalTable = logicalTable;
            this.filePath = filePath;
            this.stripeId = stripeId;
            this.workNumber = workNumber;
        }

        public static WorkId of(String workId, String logicalSchema, String logicalTable) {
            String[] parts = workId.split("\\$");

            if (parts.length != 5) {
                throw new IllegalArgumentException("Invalid workId format: " + workId);
            }

            String queryId = parts[1];
            String filePath = parts[2];
            filePath = filePath.substring(filePath.lastIndexOf("_") + 1);

            int stripeId = Integer.parseInt(parts[3]);
            int workNumber = Integer.parseInt(parts[4]);

            return new WorkId(queryId, logicalSchema, logicalTable, filePath, stripeId, workNumber);
        }
    }

    private WorkId workId;
    private int sequence;

    public ScanWorkId(WorkId workId, int sequence) {
        this.workId = workId;
        this.sequence = sequence;
    }

    public static ScanWorkId of(WorkId workId, int sequence) {
        return new ScanWorkId(workId, sequence);
    }
}
