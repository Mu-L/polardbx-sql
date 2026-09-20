package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.optimizer.ttl.TtlColFuncExprInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;

/**
 * @author chenghui.lch
 */
public class BuildPartFieldStringParams {

    protected boolean forceReturnPartFldString = false;

    /**
     * The target timezone when converting number to timestamp
     */
    protected String targetTimeZone = null;

    /**
     * <pre>
     * Label if need convert num to unix_timestamp,
     * by
     *  'FROM_UNIXTIME(xxx)' or 'FROM_UNIXTIME(xxx/1000)'
     *  </pre>
     */
    protected boolean needConvertNumberToUnixTimestamp = false;
    /**
     * <pre>
     *     Label if treat the number as UnixTimestamp with mills second unit
     * </pre>
     */
    protected boolean treatAsUnixTimestampMillsSec = false;

    /**
     * <pre>
     *     Label if treat the number as UnixTimestamp with second unit
     * </pre>
     */
    protected boolean treatAsUnixTimestampSec = false;

    /**
     * <pre>
     * Label if need convert num to date,
     * by
     *  'FROM_DAYS(xxx)'
     *  </pre>
     */
    protected boolean needConvertNumberToDate = false;
    /**
     * <pre>
     *     Label if treat the number as to_days num
     * </pre>
     */
    protected boolean treatAsToDaysNumber = false;

    /**
     * The middle-type bound value calc context
     */
    protected TtlPartitionUtil.TtlColValueCalcContext calcContext;
    protected boolean ttlColUseFuncExprEncoding = false;

    public BuildPartFieldStringParams() {
    }

    public static BuildPartFieldStringParams constructPartFieldStringParams(TtlDefinitionInfo ttlInfo, TtlPartitionUtil.TtlColValueCalcContext calcContext) {

        BuildPartFieldStringParams params = new BuildPartFieldStringParams();
        if (!ttlInfo.isTtlColUseFuncExpr()) {//use isTtlColUseExprEncoding
            return params;
        }
        TtlColFuncExprInfo funcExprInfo = ttlInfo.getTtlColFuncExprInfo();
        params.setTargetTimeZone(ttlInfo.getTtlInfoRecord().getTtlTimezone());
//        params.setTreatAsUnixTimestampSec(funcExprInfo.isTreatTtlColAsUnixTimestampSeconds());
//        params.setTreatAsUnixTimestampMillsSec(funcExprInfo.isTreatTtlColAsUnixTimestampMillSeconds());
//        params.setNeedConvertNumberToDate(funcExprInfo.isTreatTtlColAsToDaysNumber());
//        params.setNeedConvertNumberToUnixTimestamp(funcExprInfo.isTreatTtlColAsUnixTimestampSeconds()
//            || funcExprInfo.isTreatTtlColAsUnixTimestampMillSeconds());

        params.setTtlColUseFuncExprEncoding(funcExprInfo.isUseTtlColEncoding());
        params.setCalcContext(calcContext);

        return params;
    }

    public String getTargetTimeZone() {
        return targetTimeZone;
    }

    public void setTargetTimeZone(String targetTimeZone) {
        this.targetTimeZone = targetTimeZone;
    }

    public boolean isNeedConvertNumberToUnixTimestamp() {
        return needConvertNumberToUnixTimestamp;
    }

    public void setNeedConvertNumberToUnixTimestamp(boolean needConvertNumberToUnixTimestamp) {
        this.needConvertNumberToUnixTimestamp = needConvertNumberToUnixTimestamp;
    }

    public boolean isTreatAsUnixTimestampMillsSec() {
        return treatAsUnixTimestampMillsSec;
    }

    public void setTreatAsUnixTimestampMillsSec(boolean treatAsUnixTimestampMillsSec) {
        this.treatAsUnixTimestampMillsSec = treatAsUnixTimestampMillsSec;
    }

    public boolean isTreatAsUnixTimestampSec() {
        return treatAsUnixTimestampSec;
    }

    public void setTreatAsUnixTimestampSec(boolean treatAsUnixTimestampSec) {
        this.treatAsUnixTimestampSec = treatAsUnixTimestampSec;
    }

    public boolean isNeedConvertNumberToTimestampOrDate() {
        return isNeedConvertNumberToUnixTimestamp() || isNeedConvertNumberToDate();
    }

    public boolean isNeedConvertNumberToDate() {
        return needConvertNumberToDate;
    }

    public void setNeedConvertNumberToDate(boolean needConvertNumberToDate) {
        this.needConvertNumberToDate = needConvertNumberToDate;
    }

    public boolean isTreatAsToDaysNumber() {
        return treatAsToDaysNumber;
    }

    public void setTreatAsToDaysNumber(boolean treatAsToDaysNumber) {
        this.treatAsToDaysNumber = treatAsToDaysNumber;
    }

    public boolean isForceReturnPartFldString() {
        return forceReturnPartFldString;
    }

    public void setForceReturnPartFldString(boolean forceReturnPartFldString) {
        this.forceReturnPartFldString = forceReturnPartFldString;
    }

    public TtlPartitionUtil.TtlColValueCalcContext getCalcContext() {
        return calcContext;
    }

    public void setCalcContext(
        TtlPartitionUtil.TtlColValueCalcContext calcContext) {
        this.calcContext = calcContext;
    }

    public boolean isTtlColUseFuncExprEncoding() {
        return ttlColUseFuncExprEncoding;
    }

    public void setTtlColUseFuncExprEncoding(boolean ttlColUseFuncExprEncoding) {
        this.ttlColUseFuncExprEncoding = ttlColUseFuncExprEncoding;
    }
}
