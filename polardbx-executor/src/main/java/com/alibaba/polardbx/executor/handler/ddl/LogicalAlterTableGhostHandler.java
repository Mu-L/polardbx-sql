package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.executor.ddl.job.task.omc.OmcPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlInfo;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.privilege.PrivilegeKind;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableRemovePartitioning;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTableOptions;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.supportOmc30;

/**
 * @author wumu
 */
public class LogicalAlterTableGhostHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGhostHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        // 校验开关打开
        boolean enableGhostDdl = supportOmc30(executionContext) &&
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_GHOST_DDL);
        if (!enableGhostDdl) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "omc on physical tables are not supported.");
        }

        // 校验权限
        if (!executionContext.isSuperUserOrAllPrivileges() &&
            !PolarPrivilegeUtils.checkPolardbxPrivilege(executionContext, PrivilegeKind.ALTER)) {
            PrivilegeContext pc = executionContext.getPrivilegeContext();
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                "alter on polardbx", pc.getUser(), pc.getHost());
        }

        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();

        // 校验实例存在
        boolean isStorageInstExist = false;
        String ghostDdlDataNode = SQLUtils.normalizeNoTrim(sqlAlterTable.getGhostDdlDataNode());
        for (StorageInstHaContext haContext : StorageHaManager.getInstance().getStorageHaCtxCache().values()) {
            if (haContext.getStorageMasterInstId().equals(ghostDdlDataNode)) {
                isStorageInstExist = true;
                break;
            }
        }
        if (!isStorageInstExist) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                String.format("The storage instance '%s' does not exist.", ghostDdlDataNode));
        }

        // 校验物理库名和物理表名
        String physicalDbName = null;
        String physicalTableName = null;
        SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlAlterTable.getName();
        if (!sqlIdentifier.isSimple()) {
            physicalDbName = sqlIdentifier.names.get(0);
            physicalTableName = sqlIdentifier.getLastName();
        }

        if (StringUtils.isEmpty(physicalDbName) || StringUtils.isEmpty(physicalTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "The full physical database name and physical table name must be specified.");
        }

        // 校验语句
        SqlNode sqlNode = logicalDdlPlan.getNativeSqlNode();
        if (sqlNode instanceof SqlAlterTableRepartition) {
            // ignore check
        } else if (sqlNode instanceof SqlAlterTableRemovePartitioning) {
            // ignore check
        } else if (sqlNode instanceof SqlAlterTable && sqlNode.getClass() == SqlAlterTable.class) {
            // 校验 algorithm = omc
            SqlTableOptions sqlTableOptions = sqlAlterTable.getTableOptions();
            if (!(sqlTableOptions != null && sqlTableOptions.getAlgorithm() != null &&
                sqlTableOptions.getAlgorithm().getSimple().equalsIgnoreCase(Attribute.ALTER_TABLE_ALGORITHM_OMC))) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "The algorithm=omc must be specified in the ALTER TABLE statement.");
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "The ALTER TABLE statement is not supported.");
        }

        return false;
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();
        SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlAlterTable.getName();

        String ghostDdlDataNode = SQLUtils.normalizeNoTrim(sqlAlterTable.getGhostDdlDataNode());
        String physicalDbName = sqlIdentifier.names.get(0);
        String physicalTableName = sqlIdentifier.getLastName();
        String sql = OmcUtils.rewriteAlterTableSql(sqlAlterTable.getSourceSql());

        OmcUtils.validateStmt(sql);

        Map<String, List<Pair<String, String>>> sourcePhyTableNames = new HashMap<>();
        sourcePhyTableNames.put(ghostDdlDataNode,
            Collections.singletonList(new Pair<>(physicalDbName, physicalTableName)));

        ExecutableDdlJob result = new ExecutableDdlJob();

        OmcPhyDdlTask omcPhyDdlTask =
            new OmcPhyDdlTask(SystemDbHelper.DEFAULT_DB_NAME, physicalTableName, sql, executionContext.getOriginSql(),
                sourcePhyTableNames, true);

        result.addTask(omcPhyDdlTask);

        String fullTableName = DdlJobFactory.concatWithDot(ghostDdlDataNode, physicalDbName);
        fullTableName = DdlJobFactory.concatWithDot(fullTableName, physicalTableName);
        result.addExcludeResources(Sets.newHashSet(fullTableName));
        result.getExplainOnlineDdlInfo().setOnlineDdlAlgorithm(OnlineDdlInfo.DdlAlgorithm.OMC30);
        result.getExplainOnlineDdlInfo().setOnlineDdlType(OnlineDdlInfo.DdlType.ONLINE_DDL);
        result.getExplainOnlineDdlInfo().setAdviceOnlineDdlSql(executionContext.getOriginSql());

        return result;
    }
}
