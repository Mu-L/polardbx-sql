package com.alibaba.polardbx.optimizer.core.rel.ddl.data;
import lombok.Data;
import lombok.Value;

/**
 * @author chenghui.lch
 */
public class CheckTableRoutingPrepareData extends DdlPreparedData {

    protected Boolean checkIndexRouting = false;
    protected Boolean explain = false;
    protected String tableSchema = null;
    protected String tableName = null;
    protected String indexName = null;

    public CheckTableRoutingPrepareData() {
    }

    public Boolean getCheckIndexRouting() {
        return checkIndexRouting;
    }

    public void setCheckIndexRouting(Boolean checkIndexRouting) {
        this.checkIndexRouting = checkIndexRouting;
    }

    public Boolean getExplain() {
        return explain;
    }

    public void setExplain(Boolean explain) {
        this.explain = explain;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
}
