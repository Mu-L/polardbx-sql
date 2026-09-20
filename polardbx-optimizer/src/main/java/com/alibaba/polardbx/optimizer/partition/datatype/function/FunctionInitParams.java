package com.alibaba.polardbx.optimizer.partition.datatype.function;

import org.apache.calcite.sql.SqlNode;

/**
 * @author chenghui.lch
 */
public class FunctionInitParams {

    protected SqlNode paramsAst;
    protected FunctionType functionType;
    protected String functionName;
    protected String paramsContent;
    // The PartitionCount of (sub)partition by
    protected Integer partitionCount;

    public FunctionInitParams() {
    }

    public String getParamsContent() {
        return paramsContent;
    }

    public void setParamsContent(String paramsContent) {
        this.paramsContent = paramsContent;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public SqlNode getParamsAst() {
        return paramsAst;
    }

    public void setParamsAst(SqlNode paramsAst) {
        this.paramsAst = paramsAst;
    }

    public FunctionType getFunctionType() {
        return functionType;
    }

    public void setFunctionType(FunctionType functionType) {
        this.functionType = functionType;
    }

    public Integer getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(Integer partitionCount) {
        this.partitionCount = partitionCount;
    }
}
