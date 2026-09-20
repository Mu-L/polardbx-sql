package com.alibaba.polardbx.optimizer.ttl;

/**
 * @author chenhui.lch
 */
public class RefreshArcTblViewContext {

    protected TtlDefinitionInfo tarTtlInfo;
    protected String newTtlTblSchema;
    protected String newTtlTblName;

    public RefreshArcTblViewContext() {
    }

    public TtlDefinitionInfo getTarTtlInfo() {
        return tarTtlInfo;
    }

    public void setTarTtlInfo(TtlDefinitionInfo tarTtlInfo) {
        this.tarTtlInfo = tarTtlInfo;
    }

    public String getNewTtlTblName() {
        return newTtlTblName;
    }

    public void setNewTtlTblName(String newTtlTblName) {
        this.newTtlTblName = newTtlTblName;
    }

    public String getNewTtlTblSchema() {
        return newTtlTblSchema;
    }

    public void setNewTtlTblSchema(String newTtlTblSchema) {
        this.newTtlTblSchema = newTtlTblSchema;
    }
}


