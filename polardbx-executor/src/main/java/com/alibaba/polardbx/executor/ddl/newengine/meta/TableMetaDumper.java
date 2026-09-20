package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.table.DdlTableMetaInfoAccessor;
import com.alibaba.polardbx.gms.metadb.table.DdlTableMetaInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static com.alibaba.polardbx.common.ddl.newengine.DdlType.ALTER_TABLE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.CREATE_INDEX;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.CREATE_TABLE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.DROP_INDEX;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.DROP_TABLE;

public class TableMetaDumper {
    public static void dumpTableMetaWithJobRecord(DdlEngineRecord jobRecord, Long sourceJobId) {
        List<String> ddlTypeList = Lists.newArrayList(ALTER_TABLE.name(),
            CREATE_TABLE.name(),
            DROP_TABLE.name(),
            CREATE_INDEX.name(),
            DROP_INDEX.name());
        String ddlType = jobRecord.ddlType;
        String schemaName = jobRecord.schemaName;
        String tableName = jobRecord.objectName;
        Long jobId = jobRecord.jobId;
        String ddlStmt = jobRecord.ddlStmt;
        String tableMetaInfo = "";
        if (ddlTypeList.contains(jobRecord.ddlType)) {
            DdlTableMetaInfoAccessor ddlTableMetaInfoAccessor = new DdlTableMetaInfoAccessor();
            try (Connection connection = MetaDbUtil.getConnection()) {
                ddlTableMetaInfoAccessor.setConnection(connection);
                SchemaManager schemaManager =
                    OptimizerContext.getContext(jobRecord.schemaName).getLatestSchemaManager();
                try {
                    TableMeta tableMeta = schemaManager.getTable(tableName);
                    tableMetaInfo = dumpTableMeta(tableMeta);
                    if (tableMetaInfo.getBytes().length > DdlHelper.COMPRESS_THRESHOLD_SIZE * 16) {
                        tableMetaInfo = DdlHelper.compress(tableMetaInfo);
                    }
                } catch (Exception e) {
                    if (!(e instanceof TableNotFoundException)) {
                        throw e;
                    }
                }
                DdlTableMetaInfoRecord ddlTableMetaInfoRecord =
                    new DdlTableMetaInfoRecord(schemaName, tableName, ddlStmt, ddlType, jobId, sourceJobId,
                        tableMetaInfo);
                ddlTableMetaInfoAccessor.insert(ddlTableMetaInfoRecord);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "failed to dump table meta:" + e);
            }
        }
    }

    public static String dumpTableMeta(TableMeta tableMeta) {
        // columnMeta
        // indexMeta
        // partitionInfo
        TableMetaSerializer tableMetaSerializer = new TableMetaSerializer();
        String serializeObject = tableMetaSerializer.serialize(tableMeta);
        return serializeObject;
    }

}
