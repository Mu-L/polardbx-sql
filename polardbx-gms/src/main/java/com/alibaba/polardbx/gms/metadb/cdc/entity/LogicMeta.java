package com.alibaba.polardbx.gms.metadb.cdc.entity;

import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexesInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.TableConstraintsRecord;
import com.alibaba.polardbx.gms.metadb.table.TablesInfoSchemaRecord;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author ziyang.lu 2020-12-17
 */
@Data
public class LogicMeta {

    private List<LogicDbMeta> logicDbMetas;

    @Data
    public static class PhySchema {
        private String storageInstId;
        private String group;
        private String schema;
        private List<String> phyTables;
    }

    @Data
    public static class LogicDbMeta {
        private String schema;
        private String charset;
        private List<PhySchema> phySchemas;
        private List<LogicTableMeta> logicTableMetas;
    }

    @Data
    public static class LogicTableMeta {
        private String tableName;
        private int tableMode;
        private int tableType;
        private String createSql;
        private String createSql4Phy;
        private String tableCollation;
        private List<PhySchema> phySchemas;
        LogicalTableMetaDetail tableMetaDetail;
    }

    @Data
    public static class LogicalTableMetaDetail {
        public LogicalTableMetaDetail() {
        }

        public LogicalTableMetaDetail(TablesInfoSchemaRecord tablesInfoSchemaRecord,
                                      List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords,
                                      Map<String, Map<String, Object>> columnsJdbcExtInfo,
                                      List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords,
                                      List<TableConstraintsRecord> tableConstraintsRecords) {
            this.tablesInfoSchemaRecord = tablesInfoSchemaRecord;
            this.columnsInfoSchemaRecords = columnsInfoSchemaRecords;
            this.columnsJdbcExtInfo = columnsJdbcExtInfo;
            this.indexesInfoSchemaRecords = indexesInfoSchemaRecords;
            this.tableConstraintsRecords = tableConstraintsRecords;
        }

        private TablesInfoSchemaRecord tablesInfoSchemaRecord;
        private List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords;
        private Map<String, Map<String, Object>> columnsJdbcExtInfo;
        private List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords;
        private List<TableConstraintsRecord> tableConstraintsRecords;
    }
}
