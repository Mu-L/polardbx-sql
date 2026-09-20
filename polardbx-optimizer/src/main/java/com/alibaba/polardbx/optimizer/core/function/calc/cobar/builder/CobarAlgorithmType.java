package com.alibaba.polardbx.optimizer.core.function.calc.cobar.builder;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

/**
 * @author chenghui.lch
 */
public enum CobarAlgorithmType {

//    // 1=PartitionByLong
//    // 2=PartitionByString
//    // 3=PartitionByFileMap

    COBAR_PARTITION_BY_LONG(CobarAlgorithmBuilder.COBAR_PartitionByLong, "PartitionByLong", "hash"),
    COBAR_PARTITION_BY_STRING(CobarAlgorithmBuilder.COBAR_PartitionByString, "PartitionByString", "stringhash"),
    COBAR_PARTITION_BY_FILE_MAP(CobarAlgorithmBuilder.COBAR_PartitionByFileMap, "PartitionByFileMap", "enum"),
    ;

    private int algorithmIndex;
    private String algorithmFullName;
    private String algorithmAliasName;

    CobarAlgorithmType(int index,
                       String fullName,
                       String aliasName) {
        this.algorithmIndex = index;
        this.algorithmFullName = fullName;
        this.algorithmAliasName = aliasName;
    }

    public int getAlgorithmIndex() {
        return algorithmIndex;
    }

    public String getAlgorithmFullName() {
        return algorithmFullName;
    }

    public String getAlgorithmAliasName() {
        return algorithmAliasName;
    }

    public static CobarAlgorithmType getCobarAlgorithmTypeByName(String algorithmName) {

        if (COBAR_PARTITION_BY_LONG.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || COBAR_PARTITION_BY_LONG.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return CobarAlgorithmType.COBAR_PARTITION_BY_LONG;
        }

        if (COBAR_PARTITION_BY_STRING.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || COBAR_PARTITION_BY_STRING.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return CobarAlgorithmType.COBAR_PARTITION_BY_STRING;
        }

        if (COBAR_PARTITION_BY_FILE_MAP.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || COBAR_PARTITION_BY_FILE_MAP.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return CobarAlgorithmType.COBAR_PARTITION_BY_FILE_MAP;
        }

        throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
            String.format("cobar route function %s is not supported", algorithmName));
    }

}
