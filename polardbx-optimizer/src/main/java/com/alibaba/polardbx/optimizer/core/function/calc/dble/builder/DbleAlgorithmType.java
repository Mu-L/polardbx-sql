package com.alibaba.polardbx.optimizer.core.function.calc.dble.builder;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

public enum DbleAlgorithmType {

//    // 1=PartitionByLong
//    // 2=PartitionByString
//    // 3=PartitionByFileMap
//    // 4=AutoPartitionByLong
//    // 5=PartitionByPattern
//    // 6=PartitionByDate
//    // 7=PartitionByJumpConsistentHash
//
//    public static final int DBLE_PartitionByLong = 1;
//    public static final int DBLE_PartitionByString = 2;
//    public static final int DBLE_PartitionByFileMap = 3;
//    public static final int DBLE_AutoPartitionByLong = 4;
//    public static final int DBLE_PartitionByPattern = 5;
//    public static final int DBLE_PartitionByDate = 6;
//    public static final int DBLE_PartitionByJumpConsistentHash = 7;
//    public static final int DBLE_GH_PartitionByUnitMurmurHash = 8; // only used for gh

    DBLE_PARTITION_BY_LONG(DbleAlgorithmBuilder.DBLE_PartitionByLong, "PartitionByLong", "hash"),
    DBLE_PARTITION_BY_STRING(DbleAlgorithmBuilder.DBLE_PartitionByString, "PartitionByString", "stringhash"),
    DBLE_PARTITION_BY_FILE_MAP(DbleAlgorithmBuilder.DBLE_PartitionByFileMap, "PartitionByFileMap", "enum"),
    DBLE_AUTO_PARTITION_BY_LONG(DbleAlgorithmBuilder.DBLE_AutoPartitionByLong, "AutoPartitionByLong", "numberrange"),
    DBLE_PARTITION_BY_PATTERN(DbleAlgorithmBuilder.DBLE_PartitionByPattern, "PartitionByPattern", "patternrange"),
    DBLE_PARTITION_BY_DATE(DbleAlgorithmBuilder.DBLE_PartitionByDate, "PartitionByDate", "date"),
    DBLE_PARTITION_BY_JUMP_CONSISTENT_HASH(DbleAlgorithmBuilder.DBLE_PartitionByJumpConsistentHash,
        "PartitionByJumpConsistentHash", "jumpstringhash"),
    DBLE_PARTITION_BY_GH_UNIT_MURMUR_HASH(DbleAlgorithmBuilder.DBLE_GH_PartitionByUnitMurmurHash,
        "PartitionByUnitMurmurHash", "unitmurmurhash"),
    ;

    private int algorithmIndex;
    private String algorithmFullName;
    private String algorithmAliasName;

    DbleAlgorithmType(int index,
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

    public static DbleAlgorithmType getDbleAlgorithmTypeByName(String algorithmName) {

        if (DBLE_PARTITION_BY_LONG.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || DBLE_PARTITION_BY_LONG.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return DbleAlgorithmType.DBLE_PARTITION_BY_LONG;
        }

        if (DBLE_PARTITION_BY_STRING.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || DBLE_PARTITION_BY_STRING.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return DbleAlgorithmType.DBLE_PARTITION_BY_STRING;
        }

        if (DBLE_PARTITION_BY_FILE_MAP.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || DBLE_PARTITION_BY_FILE_MAP.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return DbleAlgorithmType.DBLE_PARTITION_BY_FILE_MAP;
        }

        if (DBLE_AUTO_PARTITION_BY_LONG.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || DBLE_AUTO_PARTITION_BY_LONG.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return DbleAlgorithmType.DBLE_AUTO_PARTITION_BY_LONG;
        }

        if (DBLE_PARTITION_BY_PATTERN.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || DBLE_PARTITION_BY_PATTERN.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return DbleAlgorithmType.DBLE_PARTITION_BY_PATTERN;
        }

        if (DBLE_PARTITION_BY_DATE.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || DBLE_PARTITION_BY_DATE.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return DbleAlgorithmType.DBLE_PARTITION_BY_DATE;
        }

        if (DBLE_PARTITION_BY_JUMP_CONSISTENT_HASH.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || DBLE_PARTITION_BY_JUMP_CONSISTENT_HASH.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return DbleAlgorithmType.DBLE_PARTITION_BY_JUMP_CONSISTENT_HASH;
        }

        if (DBLE_PARTITION_BY_GH_UNIT_MURMUR_HASH.getAlgorithmFullName().equalsIgnoreCase(algorithmName)
            || DBLE_PARTITION_BY_GH_UNIT_MURMUR_HASH.getAlgorithmAliasName().equalsIgnoreCase(algorithmName)) {
            return DbleAlgorithmType.DBLE_PARTITION_BY_GH_UNIT_MURMUR_HASH;
        }

        throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
            String.format("Found invalid dble algorithm name `%s`", algorithmName));
    }

}
