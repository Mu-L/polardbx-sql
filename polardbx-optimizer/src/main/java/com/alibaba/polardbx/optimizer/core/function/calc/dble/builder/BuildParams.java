package com.alibaba.polardbx.optimizer.core.function.calc.dble.builder;

import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public final class BuildParams {

    /**
     * 1=PartitionByLong
     * 2=PartitionByString
     * 3=PartitionByFileMap
     * 4=AutoPartitionByLong
     * 5=PartitionByPattern
     * 6=PartitionByDate
     * 7=PartitionByJumpConsistentHash
     */
    protected int algorithmType;

    /**
     * For PartitionByLong/AutoPartitionByLong/PartitionByDate/PartitionByPattern
     */
    protected int defaultNode;

    /**
     * For PartitionByLong/PartitionByString/PartitionByFileMap/PartitionByJumpConsistentHash
     */
    protected String partitionCount;

    /**
     * For PartitionByLong/PartitionByString/PartitionByFileMap
     */
    protected String partitionLength;

    /**
     * For PartitionByString/PartitionByJumpConsistentHash
     */
    protected String hashSlice;

    /**
     * For PartitionByPattern
     */
    protected int patternValue;

    /**
     * For PartitionByFileMap
     */
    protected int type;

    /**
     * For AutoPartitionByLong/PartitionByPattern/PartitionByFileMap
     */
    protected List<Pair<String, Integer>> partitionMappings = new ArrayList<>();

    /**
     * For AutoPartitionByLong / PartitionByPattern / PartitionByFileMap
     */
    protected String mapFileString = null;

    /**
     * For PartitionByDate
     */
    protected String dateFormat;
    protected String sBeginDate;
    protected String sEndDate;
    protected String sPartitionDay;

    /**
     * For PartitionByUnitMurmurHash
     */
    protected String isDigitCheck;
    protected String leftPaddingChar;
    protected String shardingValueMaxLength;

    /**
     * The partitionCount of partitionBy/subpartitionBy
     */
    protected String actualPartitionCount;

    public BuildParams() {
    }

    public int getDefaultNode() {
        return defaultNode;
    }

    public void setDefaultNode(int defaultNode) {
        this.defaultNode = defaultNode;
    }

    public String getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(String partitionCount) {
        this.partitionCount = partitionCount;
    }

    public String getPartitionLength() {
        return partitionLength;
    }

    public void setPartitionLength(String partitionLength) {
        this.partitionLength = partitionLength;
    }

    public String getHashSlice() {
        return hashSlice;
    }

    public void setHashSlice(String hashSlice) {
        this.hashSlice = hashSlice;
    }

    public int getPatternValue() {
        return patternValue;
    }

    public void setPatternValue(int patternValue) {
        this.patternValue = patternValue;
    }

    public void setPartitionValue(int patternValue) {
        this.patternValue = patternValue;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public List<Pair<String, Integer>> getPartitionMappings() {
        return partitionMappings;
    }

    public void setPartitionMappings(
        List<Pair<String, Integer>> partitionMappings) {
        this.partitionMappings = partitionMappings;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getsBeginDate() {
        return sBeginDate;
    }

    public void setsBeginDate(String sBeginDate) {
        this.sBeginDate = sBeginDate;
    }

    public String getsEndDate() {
        return sEndDate;
    }

    public void setsEndDate(String sEndDate) {
        this.sEndDate = sEndDate;
    }

    public String getsPartitionDay() {
        return sPartitionDay;
    }

    public void setsPartitionDay(String sPartitionDay) {
        this.sPartitionDay = sPartitionDay;
    }

    public int getAlgorithmType() {
        return algorithmType;
    }

    public void setAlgorithmType(int algorithmType) {
        this.algorithmType = algorithmType;
    }

    public String getIsDigitCheck() {
        return isDigitCheck;
    }

    public void setIsDigitCheck(String isDigitCheck) {
        this.isDigitCheck = isDigitCheck;
    }

    public String getLeftPaddingChar() {
        return leftPaddingChar;
    }

    public void setLeftPaddingChar(String leftPaddingChar) {
        this.leftPaddingChar = leftPaddingChar;
    }

    public String getShardingValueMaxLength() {
        return shardingValueMaxLength;
    }

    public void setShardingValueMaxLength(String shardingValueMaxLength) {
        this.shardingValueMaxLength = shardingValueMaxLength;
    }

    public String getMapFileString() {
        return mapFileString;
    }

    public void setMapFileString(String mapFileString) {
        this.mapFileString = mapFileString;
    }

    public String getActualPartitionCount() {
        return actualPartitionCount;
    }

    public void setActualPartitionCount(String actualPartitionCount) {
        this.actualPartitionCount = actualPartitionCount;
    }
}
