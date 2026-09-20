package com.alibaba.polardbx.executor.ddl.newengine.job;

/**
 * @author wumu
 */
public class OnlineDdlInfo {
    public enum DdlType {
        /**
         * online ddl type
         * 1. online ddl
         * 2. 非 online ddl
         * 3. 不涉及，例如 create table、drop table 等
         */
        ONLINE_DDL,
        LOCK_TABLE,
        NONE
    }

    public enum DdlAlgorithm {
        /**
         * instant by dn
         * inplace by dn
         * only change meta by cn
         * omc online modify column by cn, need backfill
         * osc online schema change by cn, need backfill
         * copy by dn
         * default
         */
        INSTANT,
        INPLACE,
        META_ONLY,
        OMC20,
        OMC30,
        OSC,
        COPY,
        DEFAULT
    }

    public enum PhysicalDdlAlgorithmType {
        /**
         * instant
         * inplace (lock = none、lock = shared、lock = exclusive)
         * copy
         * using omc
         * can convert to omc
         */
        INSTANT,
        INPLACE_AND_NONE,
        INPLACE_AND_LOCK,
        COPY,
        OMC,
        CONVERT_TO_OMC20,
        CONVERT_TO_OMC30,
        DEFAULT
    }

    public DdlType ddlType = DdlType.NONE;
    public DdlAlgorithm algorithm = DdlAlgorithm.DEFAULT;
    public DdlType adviceDdlType = DdlType.NONE;
    public DdlAlgorithm adviceAlgorithm = DdlAlgorithm.DEFAULT;
    public String adviceOnlineDdlSql = "";

    public String[] getResult() {
        return new String[] {ddlType.name(), algorithm.name()};
    }

    public String[] getAdvisorResult() {
        return new String[] {adviceDdlType.name(), adviceOnlineDdlSql, adviceAlgorithm.name()};
    }

    public void setOnlineDdlType(DdlType ddlType) {
        this.ddlType = ddlType;
        this.adviceDdlType = ddlType;
    }

    public void setOnlineDdlAlgorithm(DdlAlgorithm adviceAlgorithm) {
        this.algorithm = adviceAlgorithm;
        this.adviceAlgorithm = adviceAlgorithm;
    }

    public static boolean isLockTableAlgorithmType(PhysicalDdlAlgorithmType type) {
        return type == PhysicalDdlAlgorithmType.INPLACE_AND_LOCK || type == PhysicalDdlAlgorithmType.COPY;
    }

    public void setDdlType(DdlType ddlType) {
        this.ddlType = ddlType;
    }

    public void setAlgorithm(DdlAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    public void setAdviceDdlType(DdlType adviceDdlType) {
        this.adviceDdlType = adviceDdlType;
    }

    public void setAdviceAlgorithm(DdlAlgorithm adviceAlgorithm) {
        this.adviceAlgorithm = adviceAlgorithm;
    }

    public void setAdviceOnlineDdlSql(String adviceOnlineDdlSql) {
        this.adviceOnlineDdlSql = adviceOnlineDdlSql;
    }
}
