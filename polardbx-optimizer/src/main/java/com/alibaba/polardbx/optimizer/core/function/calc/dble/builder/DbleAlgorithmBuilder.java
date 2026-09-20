package com.alibaba.polardbx.optimizer.core.function.calc.dble.builder;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.AbstractPartitionAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.AutoPartitionByLong;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.DblePartitionAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByDate;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByFileMap;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByJumpConsistentHash;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByLong;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByPattern;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByString;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByUnitMurmurHash;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.Pair;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class DbleAlgorithmBuilder {

    // 1=PartitionByLong
    // 2=PartitionByString
    // 3=PartitionByFileMap
    // 4=AutoPartitionByLong
    // 5=PartitionByPattern
    // 6=PartitionByDate
    // 7=PartitionByJumpConsistentHash

    public static final int DBLE_PartitionByLong = 1;
    public static final int DBLE_PartitionByString = 2;
    public static final int DBLE_PartitionByFileMap = 3;
    public static final int DBLE_AutoPartitionByLong = 4;
    public static final int DBLE_PartitionByPattern = 5;
    public static final int DBLE_PartitionByDate = 6;
    public static final int DBLE_PartitionByJumpConsistentHash = 7;
    public static final int DBLE_GH_PartitionByUnitMurmurHash = 8; // only used for gh

    public static DblePartitionAlgorithm buildAlgorithm(BuildParams params) {
        /**
         * 1=PartitionByLong
         * 2=PartitionByString
         * 3=PartitionByFileMap
         * 4=AutoPartitionByLong
         * 5=PartitionByPattern
         * 6=PartitionByDate
         * 7=PartitionByJumpConsistentHash
         * 8=PartitionByUnitMurmurHash
         */
        int algorithmType = params.getAlgorithmType();
        DblePartitionAlgorithm algorithm = null;
        // actualPartitionCount is the partitions count of  (sub)partitionBy
        String actualPartitionCount = params.getActualPartitionCount();
        // In dble, actualPartitionCount is equal to the dataNodeCount
        Integer dataNodeCount = actualPartitionCount == null ? null : Integer.valueOf(actualPartitionCount);
        switch (algorithmType) {
        case DBLE_PartitionByLong://PartitionByLong
        {
            String count = params.getPartitionCount();
            String length = params.getPartitionLength();
            if (StringUtils.isEmpty(count) || StringUtils.isEmpty(length)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "Found invalid init params for algorithm dble/%s, the init params should specify partitionCount and partitionLength",
                        DbleAlgorithmType.DBLE_PARTITION_BY_LONG.getAlgorithmAliasName()));
            }
            PartitionByLong routeFunc = new PartitionByLong();
            routeFunc.setPartitionCount(count);
            routeFunc.setPartitionLength(length);
            routeFunc.setDataNodeCount(dataNodeCount);
            routeFunc.selfCheck();
            routeFunc.init();
            checkIfPartitionCountMatchAlgorithm(routeFunc, dataNodeCount);
            algorithm = routeFunc;
        }
        break;
        case DBLE_PartitionByString://PartitionByString
        {
            String count = params.getPartitionCount();
            String length = params.getPartitionLength();
            String hashSlice = params.getHashSlice(); // hashSlice default value is 0:8
            if (StringUtils.isEmpty(hashSlice)) {
                hashSlice = "0:8";
            }
            if (StringUtils.isEmpty(count) || StringUtils.isEmpty(length)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "Found invalid init params for algorithm dble/%s, the init params should specify partitionCount,partitionLength and hashSlice",
                        DbleAlgorithmType.DBLE_PARTITION_BY_STRING.getAlgorithmAliasName()));
            }
            PartitionByString routeFunc = new PartitionByString();
            routeFunc.setPartitionCount(count);
            routeFunc.setPartitionLength(length);
            routeFunc.setHashSlice(hashSlice);
            routeFunc.setDataNodeCount(dataNodeCount);
            routeFunc.selfCheck();
            routeFunc.init();
            checkIfPartitionCountMatchAlgorithm(routeFunc, dataNodeCount);
            algorithm = routeFunc;
        }
        break;
        case DBLE_PartitionByFileMap://PartitionByFileMap
        {
            int defaultNode = params.getDefaultNode();// defaultNode defaultValue is -1
            int type = params.getType(); // type defaultValue is 0
            List<Pair<String, Integer>> mappings = params.getPartitionMappings();
            String mapFileString = params.getMapFileString();
            if (StringUtils.isEmpty(mapFileString) && mappings.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "Found invalid init params for algorithm dble/%s, the init params should specify defaultNode,type and mapFile",
                        DbleAlgorithmType.DBLE_PARTITION_BY_FILE_MAP.getAlgorithmAliasName()));
            }
            PartitionByFileMap routeFunc = new PartitionByFileMap();
            routeFunc.setDefaultNode(defaultNode);
            routeFunc.setType(type);
            routeFunc.setPartitionMappings(mappings);
            routeFunc.setMapFileString(mapFileString);
            routeFunc.setDataNodeCount(dataNodeCount);
            routeFunc.selfCheck();
            routeFunc.init();
            checkIfPartitionCountMatchAlgorithm(routeFunc, dataNodeCount);
            algorithm = routeFunc;

        }
        break;
        case DBLE_AutoPartitionByLong://AutoPartitionByLong
        {
            int defaultNode = params.getDefaultNode();// defaultNode defaultValue is -1
            List<Pair<String, Integer>> mappings = params.getPartitionMappings();
            String mapFileString = params.getMapFileString();
            if (StringUtils.isEmpty(mapFileString) && mappings.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "Found invalid init params for algorithm dble/%s, the init params should specify defaultNode and mapFile",
                        DbleAlgorithmType.DBLE_AUTO_PARTITION_BY_LONG.getAlgorithmAliasName()));
            }
            AutoPartitionByLong routeFunc = new AutoPartitionByLong();
            routeFunc.setDefaultNode(defaultNode);
            routeFunc.setPartitionMappings(mappings);
            routeFunc.setMapFileString(mapFileString);
            routeFunc.setDataNodeCount(dataNodeCount);
            routeFunc.selfCheck();
            routeFunc.init();
            checkIfPartitionCountMatchAlgorithm(routeFunc, dataNodeCount);
            algorithm = routeFunc;
        }
        break;
        case DBLE_PartitionByPattern://PartitionByPattern
        {
            int defaultNode = params.getDefaultNode();// defaultNode defaultValue is -1
            int patternValue = params.getPatternValue();// partitionValue defaultValue is 1024
            List<Pair<String, Integer>> mappings = params.getPartitionMappings();
            String mapFileString = params.getMapFileString();
            if (StringUtils.isEmpty(mapFileString) && mappings.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "Found invalid init params for algorithm dble/%s, the init params should specify defaultNode,partitionValue and mapFile",
                        DbleAlgorithmType.DBLE_PARTITION_BY_PATTERN.getAlgorithmAliasName()));
            }
            PartitionByPattern routeFunc = new PartitionByPattern();
            routeFunc.setDefaultNode(defaultNode);
            routeFunc.setPatternValue(patternValue);
            routeFunc.setPartitionMappings(mappings);
            routeFunc.setMapFileString(mapFileString);
            routeFunc.setDataNodeCount(dataNodeCount);
            routeFunc.selfCheck();
            routeFunc.init();
            checkIfPartitionCountMatchAlgorithm(routeFunc, dataNodeCount);
            algorithm = routeFunc;
        }
        break;
        case DBLE_PartitionByDate://PartitionByDate
        {
            int defaultNode = params.getDefaultNode();//  defaultNode defaultValue is -1
            String dateFormat = params.getDateFormat();
            String sBeginDate = params.getsBeginDate();
            String sEndDate = params.getsEndDate();// optional
            String sPartionDay = params.getsPartitionDay();
            String partitionCount = params.getPartitionCount();
            if (StringUtils.isEmpty(dateFormat) || StringUtils.isEmpty(sBeginDate) || StringUtils.isEmpty(
                sPartionDay)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "Found invalid init params for algorithm dble/%s, the init params should contain defaultNode,dateFormat,sBeginDate,sEndDate and sPartitionDay",
                        DbleAlgorithmType.DBLE_PARTITION_BY_DATE.getAlgorithmAliasName()));
            }
            PartitionByDate routeFunc = new PartitionByDate();
            routeFunc.setDefaultNode(defaultNode);
            routeFunc.setDateFormat(dateFormat);
            routeFunc.setsBeginDate(sBeginDate);
            routeFunc.setsEndDate(sEndDate);
            routeFunc.setsPartionDay(sPartionDay);
            routeFunc.setPartitionCount(partitionCount);
            routeFunc.setDataNodeCount(dataNodeCount);
            routeFunc.selfCheck();
            routeFunc.init();
            checkIfPartitionCountMatchAlgorithm(routeFunc, dataNodeCount);
            algorithm = routeFunc;
        }
        break;
        case DBLE_PartitionByJumpConsistentHash://PartitionByJumpConsistentHash
        {
            String count = params.getPartitionCount();
            String hashSlice = params.getHashSlice();// hashSlice defaultValue is 0:0
            if (StringUtils.isEmpty(hashSlice)) {
                hashSlice = "0:0";
            }
            if (StringUtils.isEmpty(count)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "Found invalid init params for algorithm dble/%s, the init params should specify partitionCount and hashSlice",
                        DbleAlgorithmType.DBLE_PARTITION_BY_JUMP_CONSISTENT_HASH.getAlgorithmAliasName()));
            }
            PartitionByJumpConsistentHash routeFunc = new PartitionByJumpConsistentHash();
            routeFunc.setPartitionCount(Integer.valueOf(count));
            routeFunc.setHashSlice(hashSlice);
            routeFunc.setDataNodeCount(dataNodeCount);
            routeFunc.selfCheck();
            routeFunc.init();
            checkIfPartitionCountMatchAlgorithm(routeFunc, dataNodeCount);
            algorithm = routeFunc;
        }
        break;
        case DBLE_GH_PartitionByUnitMurmurHash://PartitionByUnitMurmurHash of GH
        {
            String partitionCount = params.getPartitionCount();
            String isDigitCheck = params.getIsDigitCheck();
            String leftPaddingChar = params.getLeftPaddingChar();
            String shardingValueMaxLength = params.getShardingValueMaxLength();
            if (StringUtils.isEmpty(partitionCount)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "Found invalid init params for algorithm dble/%s, the init params should specify partitionCount,isDigitCheck,leftPaddingChar and shardingValueMaxLength",
                        DbleAlgorithmType.DBLE_PARTITION_BY_GH_UNIT_MURMUR_HASH.getAlgorithmAliasName()));
            }

            PartitionByUnitMurmurHash routeFunc = new PartitionByUnitMurmurHash();
            routeFunc.setPartitionCount(partitionCount);
            // default isDigitCheck=1
            routeFunc.setIsDigitCheck(isDigitCheck);
            // default leftPaddingChar=NO
            routeFunc.setLeftPaddingChar(leftPaddingChar);
            // default shardingValueMaxLength=15
            routeFunc.setShardingValueMaxLength(shardingValueMaxLength);

            routeFunc.setDataNodeCount(dataNodeCount);
            routeFunc.selfCheck();
            routeFunc.init();
            checkIfPartitionCountMatchAlgorithm(routeFunc, dataNodeCount);
            algorithm = routeFunc;
        }
        break;
        default:
            break;
        }



        return algorithm;
    }

    protected static void checkIfPartitionCountMatchAlgorithm(DblePartitionAlgorithm initedAlgorithm,
                                                              Integer dataNodeCount) {
        boolean autoCheckPartitionCount = DynamicConfig.getInstance().isAutoCheckPartitionCountIfMatchDbleHash();
        if (dataNodeCount != null && autoCheckPartitionCount) {
            int partNumOfAlgorithm = initedAlgorithm.getPartitionNum();
            int dataNodeCountInt = dataNodeCount;
            int suitValue = initedAlgorithm.suitableFor(dataNodeCountInt);
            if (suitValue < 0) {
                String errMsg = String.format(
                    "function partition size : %s, datanode size is %s, please make sure table datanode size = function partition size",
                    partNumOfAlgorithm, dataNodeCount);
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);
            } else if (suitValue > 0) {
                //String warnMsg = String.format("function partition size : %s, datanode size is %s, table datanode size > function partition size",partNumOfAlgorithm, dataNodeCount);
                // ignore
            } else {
                // table data node size == rule function partition size
            }
        }

    }

//    /**
//     * shard table datanode(2) < function count(3) and check failed
//     */
//    private void checkRuleSuitTable(TableConfig tableConf) {
//        AbstractPartitionAlgorithm function = tableConf.getRule().getRuleAlgorithm();
//        int suitValue = function.suitableFor(tableConf.getDataNodes().size());
//        if (suitValue < 0) {
//            throw new ConfigException("Illegal table conf : table [ " + tableConf.getName() + " ] rule function [ " +
//                tableConf.getRule().getFunctionName() + " ] partition size : " + tableConf.getRule().getRuleAlgorithm().getPartitionNum() + " > table datanode size : " +
//                tableConf.getDataNodes().size() + ", please make sure table datanode size = function partition size");
//        } else if (suitValue > 0) {
//            problemReporter.warn("table conf : table [ " + tableConf.getName() + " ] rule function [ " + tableConf.getRule().getFunctionName() + " ] " +
//                "partition size : " + String.valueOf(tableConf.getRule().getRuleAlgorithm().getPartitionNum()) + " < table datanode size : " + String.valueOf(tableConf.getDataNodes().size()));
//        } else {
//            // table data node size == rule function partition size
//        }
//    }

    public static DblePartitionAlgorithm buildAlgorithmByInitParams(DbleAlgorithmInitParams initParams) {
        BuildParams dbleBuildParams = initParams.getBuildParams();
        DblePartitionAlgorithm dblePartitionAlgorithm = buildAlgorithm(dbleBuildParams);
        return dblePartitionAlgorithm;
    }
}
