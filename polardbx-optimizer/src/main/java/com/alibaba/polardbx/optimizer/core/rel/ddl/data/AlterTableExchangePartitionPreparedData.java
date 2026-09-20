package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;


@Data
@NoArgsConstructor
public class AlterTableExchangePartitionPreparedData extends DdlPreparedData {

    private String schemaName;
    private String sourceTableName;
    private String targetTableName;
    private String srcTableGroupName;
    private String targetTableGroupName;
    private Long srcTableVersion;
    private Long targetTableVersion;
    private boolean exclusiveSrcTableGroup;
    private boolean exclusiveTargetTableGroup;
    private List<String> sourcePartitionNames;
    private List<String> targetPartitionNames;
    private boolean srcPartIsSubPartition;
    private ExchangeType exchangeType;
    private boolean validate;
    private String sourceSql;

    enum ExchangeType {
        PARTITION_TO_PARTITION,
        PARTITION_TO_SINGLE
    }

}
