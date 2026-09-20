package com.alibaba.polardbx.optimizer.partition.datatype.function;

import com.alibaba.polardbx.druid.util.StringUtils;

public enum FunctionType {

    DBLE_PARAMS("dble"),
    DBLE_EXT_PARAMS("dble_ext"),
    COBAR_PARAMS("cobar"),
    COBAR_EXT_PARAMS("cobar_ext"),
    //    SHARDING_JDBC_PARAMS("sharding_jdbc"),
//    MYCAT_PARAMS("mycat"),
    NORMAL("normal");

    private String typeName;

    FunctionType(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public static FunctionType getFuncTypeByTypeName(String paramsType) {
        if (StringUtils.isEmpty(paramsType)) {
            return FunctionType.NORMAL;
        }

        if (DBLE_PARAMS.getTypeName().equalsIgnoreCase(paramsType)) {
            return FunctionType.DBLE_PARAMS;
        }

        if (DBLE_EXT_PARAMS.getTypeName().equalsIgnoreCase(paramsType)) {
            return FunctionType.DBLE_EXT_PARAMS;
        }

        if (COBAR_PARAMS.getTypeName().equalsIgnoreCase(paramsType)) {
            return FunctionType.COBAR_PARAMS;
        }

        if (COBAR_EXT_PARAMS.getTypeName().equalsIgnoreCase(paramsType)) {
            return FunctionType.COBAR_EXT_PARAMS;
        }

        return FunctionType.NORMAL;
    }
}