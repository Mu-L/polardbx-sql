package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCheck;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnCheck;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnReference;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateIndex.SqlIndexConstraintType;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlIndexOption;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMoveDatabase;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.SqlSequence;
import org.apache.calcite.sql.SqlTruncateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author minggong.zm 2018-03-06
 */
public class PhyDDLViewBuilder extends PhyOperationBuilderCommon {

    private final ExecutionContext ec;
    /**
     * SQL Template, tableName has been parameterized.
     */
    private SqlNode sqlTemplate;
    /**
     * <pre>
     * key: GroupName
     * values: List of TableNames
     * </pre>
     */
    private final Map<String, List<List<String>>> targetTables;
    private final Map<Integer, ParameterContext> params;
    private final DDL parent;
    private final DbType dbType;
    private final List<Integer> paramIndex;
    SequenceBean sequenceBean;
    /**
     * for create table with gsi
     */
    private String indexTableName;
    private SqlNode originSqlTemplate;
    private boolean ddlOnGsiTable;
    private String schemaName;
    private OptimizerContext optimizerContext;
    private String currentReplicateTableName;
    private boolean alterTableByAddingPartitions;
    private boolean alterTableBydroppingPartitions;

    public PhyDDLViewBuilder(SqlNode sqlTemplate, Map<String, List<List<String>>> targetTables,
                             Map<Integer, ParameterContext> params, DDL parent, DbType dbType, String schemaName,
                             ExecutionContext ec) {
        this.sqlTemplate = sqlTemplate;
        this.originSqlTemplate = sqlTemplate;
        this.targetTables = targetTables;
        this.params = params;
        this.parent = parent;
        this.dbType = dbType;
        this.paramIndex = new ArrayList<>();
        this.schemaName = schemaName;
        this.optimizerContext = OptimizerContext.getContext(schemaName);
        this.ec = ec;
        initParameterIndex(targetTables);
        initSqlTemplate();
    }

    private void initParameterIndex(Map<String, List<List<String>>> targetTables) {
        if (targetTables != null) {
            if (targetTables.keySet().size() == 0) {
                return;
            }
            final String next = targetTables.keySet().iterator().next();
            final List<List<String>> listSplit = targetTables.get(next);
            if (listSplit == null || listSplit.size() == 0) {
                return;
            }
            final List<String> paramCount = listSplit.get(0);
            for (int i = 0; paramCount != null && i < paramCount.size(); i++) {
                paramIndex.add(Integer.valueOf(-1));
            }
        }
    }

    public static SqlNode createIndexTable(SqlAlterTable sqlAlterTable,
                                           Map<String, SqlIndexColumnName> indexColumnMap,
                                           Map<String, SqlIndexColumnName> coveringMap,
                                           MySqlCreateTableStatement indexTableStmt, Set<String> shardingKey,
                                           DataDefLanguageLogicView parent,
                                           String schemaName, ExecutionContext ec) {
        final SqlAddIndex addIndex = (SqlAddIndex) sqlAlterTable.getAlters().get(0);
        final SqlIdentifier indexTableName = addIndex.getIndexDef().getIndexName();
        final boolean unique = addIndex instanceof SqlAddUniqueIndex;
        final List<SqlIndexOption> options = addIndex.getIndexDef().getOptions();
        final boolean isClusteredIndex = addIndex.isClusteredIndex();

        if (isClusteredIndex) {
            return createClusteredIndexTable(indexTableName,
                indexColumnMap,
                indexTableStmt,
                unique,
                options,
                parent,
                sqlAlterTable,
                schemaName, ec);
        } else {
            return createIndexTable(indexTableName,
                indexColumnMap,
                coveringMap,
                indexTableStmt,
                unique,
                options,
                shardingKey,
                parent,
                sqlAlterTable,
                schemaName, ec);
        }
    }

    /**
     * build SqlCreateTable for index table, add primary key and sharding key to
     * covering by default;
     */
    private static SqlNode createIndexTable(SqlIdentifier indexTableName,
                                            Map<String, SqlIndexColumnName> indexColumnMap,
                                            Map<String, SqlIndexColumnName> coveringMap,
                                            MySqlCreateTableStatement indexTableStmt, boolean unique,
                                            List<SqlIndexOption> options, Set<String> shardingKey,
                                            DataDefLanguageLogicView parent, SqlAlterTable sqlAlterTable,
                                            String schemaName, ExecutionContext ec) {
        final String gsiName = indexTableName.getLastName();

        // update index table name
        indexTableStmt.setTableName(SqlIdentifier.surroundWithBacktick(gsiName));

        final Iterator<SQLTableElement> it = indexTableStmt.getTableElementList().iterator();
        final Set<String> sortedCovering = new LinkedHashSet<>();
        final Set<String> fullColumn = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final Set<String> indexColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumns.addAll(indexColumnMap.keySet());
        final Set<String> coveringColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        coveringColumns.addAll(coveringMap.keySet());
        boolean withoutPk = true;
        String onUpdate = null;
        String defaultCurrentTime = null;
        String timestampWithoutDefault = null;
        String duplicatedIndexName = null;

        /**
         * <pre>
         *     1. remove columns not included in sharding key or covering columns
         *     2. remove AUTO_INCREMENT property
         *     3. remove indexes with columns not included
         *     4. remove foreign key
         *     5. check primary key exists
         *     6. check no DEFAULT CURRENT_TIMESTAMP specified for index or covering column
         *     7. check no ON UPDATE CURRENT_TIMESTAMP specified for index or covering column
         *     8. check all timestamp type columns has default value other than CURRENT_TIMESTAMP
         *     9. remove check constraint
         * </pre>
         */
        while (it.hasNext()) {
            final SQLTableElement tableElement = it.next();
            if (tableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                final String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());

                if (!columnDefinition.isPrimaryKey() && !coveringColumns.contains(columnName)
                    && !indexColumns.contains(columnName) && !shardingKey.contains(columnName)) {
                    it.remove();
                } else {
                    final boolean addToCovering =
                        !indexColumns.contains(columnName) && (coveringColumns.contains(columnName) || shardingKey
                            .contains(columnName) || columnDefinition.isPrimaryKey());
                    if (addToCovering) {
                        sortedCovering.add(columnName);
                    }
                    fullColumn.add(columnName);

                    if (!(sqlAlterTable instanceof SqlAlterTablePartitionKey) && columnDefinition.isAutoIncrement()) {
                        columnDefinition.setAutoIncrement(false);
                    }

                    if (null != columnDefinition.getConstraints()) {
                        final Iterator<SQLColumnConstraint> constraintIt = columnDefinition.getConstraints().iterator();
                        while (constraintIt.hasNext()) {
                            final SQLColumnConstraint constraint = constraintIt.next();
                            if (constraint instanceof SQLColumnPrimaryKey) {
                                withoutPk = false;
                            } else if (constraint instanceof SQLColumnReference) {
                                // remove foreign key
                                constraintIt.remove();
                            }
                        }
                    }

                    final SQLExpr defaultExpr = columnDefinition.getDefaultExpr();
                    defaultCurrentTime = extractCurrentTimestamp(defaultCurrentTime, defaultExpr);

                    onUpdate = extractCurrentTimestamp(onUpdate, columnDefinition.getOnUpdate());

                    if ("timestamp".equalsIgnoreCase(columnDefinition.getDataType().getName()) && null == defaultExpr) {
                        timestampWithoutDefault = columnName;
                    }
                }
            } else if (tableElement instanceof MySqlPrimaryKey) {
                withoutPk = false;
            } else if (tableElement instanceof MySqlKey) {
                final MySqlKey key = (MySqlKey) tableElement;

                final String indexName = ((SQLIdentifierExpr) key.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

                for (SQLSelectOrderByItem column : key.getColumns()) {
                    final String columnName = SqlCreateTable.getIndexColumnName(column);
                    if (columnName == null) {
                        it.remove();
                        break;
                    }
                    if (fullColumn.contains(columnName)) {
                        continue;
                    }

                    it.remove();
                    break;
                }
            } else if (tableElement instanceof MySqlTableIndex) {
                final MySqlTableIndex tableIndex = (MySqlTableIndex) tableElement;

                final String indexName = ((SQLIdentifierExpr) tableIndex.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

                for (SQLSelectOrderByItem column : tableIndex.getColumns()) {
                    final String columnName = SqlCreateTable.getIndexColumnName(column);
                    if (columnName == null) {
                        it.remove();
                        break;
                    }
                    if (fullColumn.contains(columnName)) {
                        continue;
                    }

                    it.remove();
                    break;
                }
            } else if (tableElement instanceof MysqlForeignKey) {
                final MysqlForeignKey foreignKye = (MysqlForeignKey) tableElement;

                final String indexName = ((SQLIdentifierExpr) foreignKye.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

                it.remove();
            } else if (tableElement instanceof SQLCheck) {
                final SQLCheck sqlCheck = (SQLCheck) tableElement;
                final String indexName = ((SQLIdentifierExpr) sqlCheck.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }
                it.remove();
            }
        }

        final PlannerContext context = (PlannerContext) parent.getCluster().getPlanner().getContext();
        final boolean defaultCurrentTimestamp =
            context.getParamManager().getBoolean(ConnectionParams.GSI_DEFAULT_CURRENT_TIMESTAMP);
        final boolean onUpdateCurrentTimestamp =
            context.getParamManager().getBoolean(ConnectionParams.GSI_ON_UPDATE_CURRENT_TIMESTAMP);

        if (withoutPk) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                "need primary key");
        }

        if (null != defaultCurrentTime && !defaultCurrentTimestamp) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "cannot use DEFAULT " + defaultCurrentTime + " on index or covering column");
        }

        if (null != onUpdate && !onUpdateCurrentTimestamp) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "cannot use ON UPDATE " + onUpdate + " on index or covering column");
        }

        if (null != timestampWithoutDefault && (!defaultCurrentTimestamp || !onUpdateCurrentTimestamp)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "need default value other than CURRENT_TIMESTAMP for column `" + timestampWithoutDefault + "`");
        }

        if (null != duplicatedIndexName) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "Duplicate index name '" + duplicatedIndexName + "'");
        }

        // rebuild covering columns
        if (parent.getNativeSqlNode() instanceof SqlCreateIndex) {
            final SqlCreateIndex createIndex = (SqlCreateIndex) parent.getNativeSqlNode();
            parent.setNativeSqlNode(createIndex.rebuildCovering(sortedCovering));
        } else if (parent.getNativeSqlNode() instanceof SqlAlterTable) {
            final SqlAlterTable alterTable = (SqlAlterTable) parent.getNativeSqlNode();
            final SqlAddIndex oldAddIndex = (SqlAddIndex) alterTable.getAlters().get(0);
            final SqlIndexDefinition oldIndexDef = oldAddIndex.getIndexDef();
            final SqlIndexDefinition newIndexDef = oldIndexDef.replaceCovering(sortedCovering);

            updateAddIndex(alterTable, 0, oldAddIndex, newIndexDef);
        } else if (parent.getNativeSqlNode() instanceof SqlCreateTable) {
            final SqlAddIndex addIndex = (SqlAddIndex) sqlAlterTable.getAlters().get(0);
            final SqlIndexDefinition oldIndexDef = addIndex.getIndexDef();
            final SqlIndexDefinition newIndexDef = oldIndexDef.replaceCovering(sortedCovering);

            updateAddIndex(sqlAlterTable, 0, addIndex, newIndexDef);
        }

        final List<String> indexShardKey = parent instanceof PartitionTableDdlView ?
            (parent.getNativeSqlNode().getKind() == SqlKind.CREATE_INDEX) ?
                ((PartitionTableDdlView) parent).getPartitionInfo().getPartitionBy().getPartitionColumnNameList() :
                (((PartitionTableDdlView) parent).getAllGsiPartitionInfos().get(indexTableName.getSimple())
                    .getPartitionBy()
                    .getPartitionColumnNameList()) :
            parent.getGsiTableRules().get(indexTableName.getSimple()).getShardColumns();
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            SqlCreateTable.addCompositeIndex(indexColumnMap, indexTableStmt, unique, options, true, indexShardKey,
                false, "");
        } else {
            SqlCreateTable.addIndex(indexColumnMap, indexTableStmt, unique, options, true, indexShardKey);
        }

        final SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);
        final SequenceBean sequenceBean = FastSqlConstructUtils.convertTableElements(columnList,
            indexTableStmt.getTableElementList(), new ContextParameters(false), ec);
        if (sequenceBean != null) {
            sequenceBean.setSchemaName(schemaName);
        }

        final String createIndexTable = SQLUtils.toSQLString(indexTableStmt, com.alibaba.polardbx.druid.DbType.mysql);

        final SqlCreateTable result = SqlDdlNodes.createTable(SqlParserPos.ZERO,
            false,
            false,
            indexTableName,
            null,
            columnList,
            null,
            null,
            null,
            null,
            null,
            createIndexTable,
            false,
            sequenceBean,
            null,
            null,
            null,
            null,
            null);

        result.setUniqueShardingKey(unique);

        ReplaceTableNameWithQuestionMarkVisitor visitor = new ReplaceTableNameWithQuestionMarkVisitor(schemaName, ec);
        return result.accept(visitor);
    }

    /**
     * build SqlCreateTable for clustered index table, add sharding key to index;
     */
    private static SqlNode createClusteredIndexTable(SqlIdentifier indexTableName,
                                                     Map<String, SqlIndexColumnName> indexColumnMap,
                                                     MySqlCreateTableStatement indexTableStmt, boolean unique,
                                                     List<SqlIndexOption> options, DataDefLanguageLogicView parent,
                                                     SqlAlterTable sqlAlterTable, String schemaName,
                                                     ExecutionContext ec) {
        final String gsiName = indexTableName.getLastName();

        // update index table name
        indexTableStmt.setTableName(SqlIdentifier.surroundWithBacktick(gsiName));
        final Set<String> sortedCovering = new LinkedHashSet<>();
        final Set<String> indexColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumns.addAll(indexColumnMap.keySet());

        final Iterator<SQLTableElement> it = indexTableStmt.getTableElementList().iterator();

        boolean withoutPk = true;
        String onUpdate = null;
        String defaultCurrentTime = null;
        String timestampWithoutDefault = null;
        String duplicatedIndexName = null;

        /**
         * <pre>
         *     1. remove AUTO_INCREMENT property
         *     2. remove foreign key
         *     3. check primary key exists
         *     4. check no DEFAULT CURRENT_TIMESTAMP specified for index or covering column
         *     5. check no ON UPDATE CURRENT_TIMESTAMP specified for index or covering column
         *     6. check all timestamp type columns has default value other than CURRENT_TIMESTAMP
         *     7. remove check constraint
         * </pre>
         */
        while (it.hasNext()) {
            final SQLTableElement tableElement = it.next();
            if (tableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                final String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());

                if (!indexColumns.contains(columnName)) {
                    sortedCovering.add(columnName);
                }
                if (columnDefinition.isAutoIncrement()) {
                    columnDefinition.setAutoIncrement(false);
                }

                if (null != columnDefinition.getConstraints()) {
                    final Iterator<SQLColumnConstraint> constraintIt = columnDefinition.getConstraints().iterator();
                    while (constraintIt.hasNext()) {
                        final SQLColumnConstraint constraint = constraintIt.next();
                        if (constraint instanceof SQLColumnPrimaryKey) {
                            withoutPk = false;
                        } else if (constraint instanceof SQLColumnReference) {
                            // remove foreign key
                            constraintIt.remove();
                        } else if (constraint instanceof SQLColumnCheck) {
                            // remove check constraint
                            constraintIt.remove();
                        }
                    }
                }
                // 暂时不限制defaultCurrentTime，
//                final SQLExpr defaultExpr = columnDefinition.getDefaultExpr();
//                defaultCurrentTime = extractCurrentTimestamp(defaultCurrentTime, defaultExpr);
//
//                onUpdate = extractCurrentTimestamp(onUpdate, columnDefinition.getOnUpdate());
//
//                if ("timestamp".equalsIgnoreCase(columnDefinition.getDataType().getName()) && null == defaultExpr) {
//                    timestampWithoutDefault = columnName;
//                }

            } else if (tableElement instanceof MySqlPrimaryKey) {
                withoutPk = false;
            } else if (tableElement instanceof MySqlKey) {
                final MySqlKey key = (MySqlKey) tableElement;

                final String indexName = ((SQLIdentifierExpr) key.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

            } else if (tableElement instanceof MySqlTableIndex) {
                final MySqlTableIndex tableIndex = (MySqlTableIndex) tableElement;

                final String indexName = ((SQLIdentifierExpr) tableIndex.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

            } else if (tableElement instanceof MysqlForeignKey) {
                final MysqlForeignKey foreignKye = (MysqlForeignKey) tableElement;

                final String indexName = ((SQLIdentifierExpr) foreignKye.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }

                it.remove();
            } else if (tableElement instanceof SQLCheck) {
                final SQLCheck sqlCheck = (SQLCheck) tableElement;
                final String indexName = ((SQLIdentifierExpr) sqlCheck.getName()).normalizedName();
                if (TStringUtil.equalsIgnoreCase(indexName, gsiName)) {
                    duplicatedIndexName = indexName;
                }
                it.remove();
            }
        }

        if (withoutPk) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                "need primary key");
        }

        if (null != defaultCurrentTime) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "cannot use DEFAULT " + defaultCurrentTime + " on index or covering column");
        }

        if (null != onUpdate) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "cannot use ON UPDATE " + onUpdate + " on index or covering column");
        }

        if (null != timestampWithoutDefault) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "need default value other than CURRENT_TIMESTAMP for column `" + timestampWithoutDefault + "`");
        }

        if (null != duplicatedIndexName) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION,
                "Duplicate index name '" + duplicatedIndexName + "'");
        }

        // rebuild covering columns
        if (parent.getNativeSqlNode() instanceof SqlCreateIndex) {
            final SqlCreateIndex createIndex = (SqlCreateIndex) parent.getNativeSqlNode();
            parent.setNativeSqlNode(createIndex.rebuildCovering(sortedCovering));
        } else if (parent.getNativeSqlNode() instanceof SqlAlterTable) {
            final SqlAlterTable alterTable = (SqlAlterTable) parent.getNativeSqlNode();
            final SqlAddIndex oldAddIndex = (SqlAddIndex) alterTable.getAlters().get(0);
            final SqlIndexDefinition oldIndexDef = oldAddIndex.getIndexDef();
            final SqlIndexDefinition newIndexDef = oldIndexDef.replaceCovering(sortedCovering);

            updateAddIndex(alterTable, 0, oldAddIndex, newIndexDef);
        } else if (parent.getNativeSqlNode() instanceof SqlCreateTable) {
            final SqlAddIndex addIndex = (SqlAddIndex) sqlAlterTable.getAlters().get(0);
            final SqlIndexDefinition oldIndexDef = addIndex.getIndexDef();
            final SqlIndexDefinition newIndexDef = oldIndexDef.replaceCovering(sortedCovering);

            updateAddIndex(sqlAlterTable, 0, addIndex, newIndexDef);
        }

        final List<String> indexShardKey = parent.getGsiTableRules().get(indexTableName.getSimple()).getShardColumns();
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            SqlCreateTable.addCompositeIndex(indexColumnMap, indexTableStmt, unique, options, true, indexShardKey,
                false, "");
        } else {
            SqlCreateTable.addIndex(indexColumnMap, indexTableStmt, unique, options, true, indexShardKey);
        }

        final SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);
        final SequenceBean sequenceBean = FastSqlConstructUtils.convertTableElements(columnList,
            indexTableStmt.getTableElementList(),
            new ContextParameters(false), ec);
        if (sequenceBean != null) {
            sequenceBean.setSchemaName(schemaName);
        }

        final String createIndexTable = SQLUtils.toSQLString(indexTableStmt, com.alibaba.polardbx.druid.DbType.mysql);

        final SqlCreateTable result = SqlDdlNodes.createTable(SqlParserPos.ZERO,
            false,
            false,
            indexTableName,
            null,
            columnList,
            null,
            null,
            null,
            null,
            null,
            createIndexTable,
            false,
            sequenceBean,
            null,
            null,
            null,
            null,
            null);

        result.setUniqueShardingKey(unique);

        ReplaceTableNameWithQuestionMarkVisitor visitor = new ReplaceTableNameWithQuestionMarkVisitor(schemaName, ec);
        return result.accept(visitor);
    }

    private static void updateAddIndex(SqlAlterTable sqlAlterTable, int itemIndex, SqlAddIndex addIndex,
                                       SqlIndexDefinition newIndexDef) {
        switch (addIndex.getKind()) {
        case ADD_UNIQUE_INDEX:
            sqlAlterTable.getAlters().set(itemIndex,
                new SqlAddUniqueIndex(addIndex.getParserPosition(), addIndex.getIndexName(), newIndexDef));
            break;
        case ADD_FULL_TEXT_INDEX:
        case ADD_SPATIAL_INDEX:
            // TODO handle FULL TEXT/SPATIAL INDEX
        case ADD_INDEX:
        default:
            sqlAlterTable.getAlters().set(itemIndex,
                new SqlAddIndex(addIndex.getParserPosition(), addIndex.getIndexName(), newIndexDef));
            break;
        }
    }

    private static String extractCurrentTimestamp(String onUpdate, SQLExpr onUpdateExpr) {
        if (onUpdateExpr instanceof SQLCurrentTimeExpr || onUpdateExpr instanceof SQLMethodInvokeExpr) {
            try {
                if (onUpdateExpr instanceof SQLMethodInvokeExpr) {
                    SQLCurrentTimeExpr.Type.valueOf(((SQLMethodInvokeExpr) onUpdateExpr).getMethodName().toUpperCase());
                    onUpdate = SQLUtils.toMySqlString(onUpdateExpr);
                } else {
                    onUpdate = ((SQLCurrentTimeExpr) onUpdateExpr).getType().name;
                }
            } catch (Exception e) {
                // ignore error for ON UPDATE CURRENT_TIMESTAMP(3);
            }
        }
        return onUpdate;
    }

    private void initSqlTemplate() {
        if (sqlTemplate instanceof SqlCreateTable) {
            final SqlCreateTable sqlTemplate = (SqlCreateTable) this.sqlTemplate;
            this.sqlTemplate = SqlDdlNodes.createTable(this.sqlTemplate.getParserPosition(),
                sqlTemplate.isReplace(),
                sqlTemplate.isIfNotExists(),
                sqlTemplate.getName(),
                null,
                sqlTemplate.getColumnList(),
                sqlTemplate.getQuery(),
                null,
                null,
                null,
                null,
                sqlTemplate.rewrite().toString(),
                false,
                sqlTemplate.getAutoIncrement(),
                null,
                null,
                null,
                null,
                null);
            ((SqlCreateTable) this.sqlTemplate).setTemporary(sqlTemplate.isTemporary());

            if (!ConfigDataMode.isPolarDbX() &&
                !sqlTemplate.isBroadCast() &&
                sqlTemplate.getDbpartitionBy() == null &&
                sqlTemplate.getTbpartitionBy() == null) {
                // Only single table supports foreign key.
                ((SqlCreateTable) this.sqlTemplate).setLogicalReferencedTables(
                    sqlTemplate.getLogicalReferencedTables());
                ((SqlCreateTable) this.sqlTemplate).setPhysicalReferencedTables(
                    sqlTemplate.getPhysicalReferencedTables());
            }

            sequenceBean = sqlTemplate.getAutoIncrement();
        } else if (sqlTemplate instanceof SqlAlterTable) {
            final SqlAlterTable alterTable = (SqlAlterTable) sqlTemplate;
            final boolean isRepartitionReq = alterTable instanceof SqlAlterTablePartitionKey;

            sequenceBean = alterTable.getAutoIncrement();

            if (alterTable.createGsi()) {
                final SqlAddIndex addIndex = (SqlAddIndex) alterTable.getAlters().get(0);
                if (null != addIndex.getIndexDef().getPrimaryTableDefinition()) {

                    // no sequence info for index table
                    this.sequenceBean = null;

                    // build index table definition from primary table
                    // definition
                    this.sqlTemplate = buildIndexTableDefinition(alterTable, isRepartitionReq);
                }
            }
        } else if (sqlTemplate instanceof SqlSequence) {
            sequenceBean = ((SqlSequence) sqlTemplate).getSequenceBean();
        } else if (sqlTemplate instanceof SqlCreateIndex && ((SqlCreateIndex) sqlTemplate).createGsi()
            && null != ((SqlCreateIndex) sqlTemplate).getPrimaryTableDefinition()) {

            // build index table definition from primary table definition
            this.sqlTemplate = buildIndexTableDefinition((SqlCreateIndex) this.sqlTemplate);
        }
    }

    private SqlNode buildIndexTableDefinition(final SqlAlterTable sqlAlterTable, final boolean forceAllowGsi) {
        final SqlIndexDefinition indexDef = ((SqlAddIndex) sqlAlterTable.getAlters().get(0)).getIndexDef();

        final DataDefLanguageLogicView ddl = (DataDefLanguageLogicView) parent;

        /**
         * build global secondary index table
         */

        final List<SqlIndexColumnName> covering =
            indexDef.getCovering() == null ? new ArrayList<>() : indexDef.getCovering();
        final Map<String, SqlIndexColumnName> coveringMap = Maps.uniqueIndex(covering,
            SqlIndexColumnName::getColumnNameStr);
        final Map<String, SqlIndexColumnName> indexColumnMap = new LinkedHashMap<>(indexDef.getColumns().size());
        indexDef.getColumns().forEach(cn -> indexColumnMap.putIfAbsent(cn.getColumnNameStr(), cn));
        final String primaryTableName = sqlAlterTable.getOriginTableName().getLastName();
        /**
         * for CREATE TABLE with GSI, primaryRule is null, cause at this point
         * primary table has not been created
         */
        final TableRule primaryRule = Optional.ofNullable(OptimizerContext.getContext(schemaName)
            .getRuleManager()
            .getTableRule(primaryTableName)).orElse(ddl.getTableRule());

        /**
         * check if index columns contains all sharding columns
         */
        final TableRule indexRule = ddl.getGsiTableRules().get(indexDef.getIndexName().getLastName());

        final Set<String> indexColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumnSet.addAll(indexColumnMap.keySet());
        // Columnar index do not force using index column as partition column
        final boolean isColumnar = indexDef.isColumnar();
        if (!isColumnar
            && !containsAllShardingColumns(indexColumnSet, indexRule)
            && indexDef.getPartitioning() == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INDEX_AND_SHARDING_COLUMNS_NOT_MATCH);
        }

        /**
         * check single/broadcast table
         */
        if (null != primaryRule) {
            final boolean singleTable = GeneralUtil.isEmpty(primaryRule.getDbShardRules())
                && GeneralUtil.isEmpty(primaryRule.getTbShardRules());
            if (!forceAllowGsi && !isColumnar && (primaryRule.isBroadcast() || singleTable)) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                    "Does not support create Global Secondary Index on single or broadcast table");
            }
        }

        /**
         * copy table structure from main table
         */
        final MySqlCreateTableStatement astCreateIndexTable =
            (MySqlCreateTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(
                    indexDef.getPrimaryTableDefinition(),
                    JdbcConstants.MYSQL)
                .get(0)
                .clone();

        assert primaryRule != null;
        final Set<String> shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (indexDef.getPartitioning() != null) {
            assert parent instanceof PartitionTableDdlView;
            PartitionInfo primaryPartitionInfo = ((PartitionTableDdlView) parent).getPartitionInfo();
            shardingColumns.addAll(primaryPartitionInfo.getPartitionColumns());
        } else {
            shardingColumns.addAll(primaryRule.getShardColumns());
        }

        return createIndexTable(sqlAlterTable,
            indexColumnMap,
            coveringMap,
            astCreateIndexTable,
            shardingColumns,
            ddl,
            schemaName, ec);
    }

    private static boolean containsAllShardingColumns(Set<String> indexColumnSet, TableRule indexRule) {
        boolean result = false;

        if (null != indexRule) {
            List<String> shardColumns = indexRule.getShardColumns();
            if (null != shardColumns) {
                result = indexColumnSet.containsAll(shardColumns);
            }
        }

        return result;
    }

    private SqlNode buildIndexTableDefinition(final SqlCreateIndex sqlCreateIndex) {
        /**
         * build global secondary index table
         */

        final Map<String, SqlIndexColumnName> coveringMap =
            null == sqlCreateIndex.getCovering() ? new HashMap<>() : Maps.uniqueIndex(sqlCreateIndex.getCovering(),
                SqlIndexColumnName::getColumnNameStr);
        final Map<String, SqlIndexColumnName> indexColumnMap = new LinkedHashMap<>(sqlCreateIndex.getColumns().size());
        sqlCreateIndex.getColumns().forEach(cn -> indexColumnMap.putIfAbsent(cn.getColumnNameStr(), cn));

        /**
         * check if index columns contains all sharding columns
         */
        final DataDefLanguageLogicView ddl = (DataDefLanguageLogicView) parent;
        final SqlIdentifier indexName = sqlCreateIndex.getIndexName();
        final TableRule indexRule = ddl.getGsiTableRules().get(indexName.getLastName());
        final String primaryTableName = sqlCreateIndex.getOriginTableName().getLastName();
        final TableRule primaryRule =
            OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(primaryTableName);

        final Set<String> indexColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexColumnSet.addAll(indexColumnMap.keySet());
        // Columnar index do not force using index column as partition column
        final boolean isColumnar = sqlCreateIndex.createCci();
        if (!isColumnar
            && !containsAllShardingColumns(indexColumnSet, indexRule)
            && sqlCreateIndex.getPartitioning() == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INDEX_AND_SHARDING_COLUMNS_NOT_MATCH);
        }

        /**
         * check single/broadcast table
         */
        if (null != primaryRule) {
            final boolean singleTable = GeneralUtil.isEmpty(primaryRule.getDbShardRules())
                && GeneralUtil.isEmpty(primaryRule.getTbShardRules());
            if (!isColumnar && primaryRule.isBroadcast() || singleTable) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION,
                    "Does not support create Global Secondary Index on single or broadcast table");
            }
        }

        /**
         * copy table structure from main table
         */
        final MySqlCreateTableStatement indexTableStmt =
            (MySqlCreateTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(
                    sqlCreateIndex.getPrimaryTableDefinition(),
                    JdbcConstants.MYSQL)
                .get(0)
                .clone();

        final boolean unique = sqlCreateIndex.getConstraintType() != null
            && sqlCreateIndex.getConstraintType() == SqlIndexConstraintType.UNIQUE;
        assert primaryRule != null;
        final Set<String> shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        if (sqlCreateIndex.getPartitioning() != null) {
            PartitionInfo primaryPartitionInfo = OptimizerContext.getContext(schemaName).getPartitionInfoManager()
                .getPartitionInfo(sqlCreateIndex.getOriginTableName().getLastName());
            shardingColumns.addAll(primaryPartitionInfo.getPartitionColumns());
        } else {
            shardingColumns.addAll(primaryRule.getShardColumns());
        }

        final boolean isClusteredIndex = sqlCreateIndex.createClusteredIndex();

        if (isClusteredIndex) {
            return createClusteredIndexTable(indexName,
                indexColumnMap,
                indexTableStmt,
                unique,
                sqlCreateIndex.getOptions(),
                ddl,
                null,
                schemaName, ec);
        } else {
            return createIndexTable(indexName,
                indexColumnMap,
                coveringMap,
                indexTableStmt,
                unique,
                sqlCreateIndex.getOptions(),
                shardingColumns,
                ddl,
                null,
                schemaName, ec);
        }
    }

    public List<RelNode> build() {
        boolean isHint = false;
        if (parent != null && parent instanceof DataDefLanguageLogicView) {
            if (((DataDefLanguageLogicView) parent).getTargetTablesHintCache() != null) {
                isHint = true;
            }
        }

        String logicalTableName = null;
        TableRule tableRule = ((DataDefLanguageLogicView) parent).getTableRule();
        PartitionInfo partitionInfo = null;
        if (parent instanceof PartitionTableDdlView) {
            partitionInfo = ((PartitionTableDdlView) parent).getPartitionInfo();
        }
        boolean isPartitioned = parent.isPartition(); // TODO handle this
        SqlNode newTableName = parent.getNewTableName();
        if (parent.sqlNode instanceof SqlCreate && ((SqlCreate) parent.sqlNode).createGsi()) {
            if (parent.sqlNode instanceof SqlCreateTable) {
                if (originSqlTemplate.getKind() == SqlKind.ALTER_TABLE) {
                    logicalTableName = indexTableName;
                    if (partitionInfo == null) {
                        tableRule = ((DataDefLanguageLogicView) parent).getGsiTableRules().get(indexTableName);
                        isPartitioned = GeneralUtil.isNotEmpty(tableRule.getDbPartitionKeys())
                            || GeneralUtil.isNotEmpty(tableRule.getTbPartitionKeys());
                    } else {
                        isPartitioned = true;
                    }
                } else {
                    logicalTableName = Util.last(((SqlIdentifier) parent.getTableName()).names);
                }
            } else if (parent.sqlNode instanceof SqlCreateIndex) {
                logicalTableName = ((SqlCreateIndex) parent.sqlNode).getIndexName().getLastName();
            } else if (parent.sqlNode instanceof SqlAlterTable) {
                final SqlAddIndex addIndex = (SqlAddIndex) ((SqlAlterTable) parent.sqlNode).getAlters().get(0);
                logicalTableName = addIndex.getIndexDef().getIndexName().getLastName();
            }
        } else if (parent.sqlNode instanceof SqlAlterTable && ((SqlAlterTable) parent.sqlNode).dropIndex()
            && sqlTemplate instanceof SqlDropTable) {
            // ALTER TABLE DROP GSI
            logicalTableName = indexTableName;
        } else if (parent.sqlNode instanceof SqlDropIndex && sqlTemplate instanceof SqlDropTable && isDdlOnGsiTable()) {
            // DROP GSI
            logicalTableName = indexTableName;
        } else if (parent.sqlNode instanceof SqlDropTable && isDdlOnGsiTable()) {
            // DROP TABLE with GSI
            logicalTableName = indexTableName;
        } else if (parent.sqlNode instanceof SqlTruncateTable && isDdlOnGsiTable()) {
            // TRUNCATE TABLE with GSI
            logicalTableName = indexTableName;
        } else if (parent.sqlNode instanceof SqlAlterTable && ((SqlAlterTable) parent.sqlNode).renameIndex()
            && sqlTemplate instanceof SqlRenameTable) {
            // ALTER TABLE RENAME GSI
            logicalTableName = indexTableName;
            newTableName = ((DataDefLanguageLogicView) parent).gsiNewIndexName.get(indexTableName);
        } else if (parent.sqlNode instanceof SqlAlterTable && isDdlOnGsiTable()) {
            // ALTER TABLE
            logicalTableName = indexTableName;
        } else if (parent.sqlNode instanceof SqlMoveDatabase) {
            logicalTableName = getCurrentReplicateTableName();
        } else if (parent.sqlNode instanceof SqlAlterTable && isAlterTableByAddingPartitions()) {
            logicalTableName = Util.last(((SqlIdentifier) parent.getTableName()).names);
        } else if ((parent.sqlNode instanceof SqlAlterTable || parent.sqlNode instanceof SqlDropIndex
            || parent.sqlNode instanceof SqlCreateIndex) &&
            (sqlTemplate instanceof SqlDropIndex || sqlTemplate instanceof SqlCreateIndex
                || sqlTemplate instanceof SqlAlterTable)) {
            // This is generated add/drop of local index for partition table.
            if (sqlTemplate instanceof SqlDropIndex) {
                logicalTableName = Util.last(((SqlDropIndex) sqlTemplate).getOriginTableName().names);
            } else if (sqlTemplate instanceof SqlCreateIndex) {
                logicalTableName = Util.last(((SqlCreateIndex) sqlTemplate).getOriginTableName().names);
            } else {
                logicalTableName = Util.last(((SqlAlterTable) sqlTemplate).getOriginTableName().names);
            }
        } else {
            logicalTableName = Util.last(((SqlIdentifier) parent.getTableName()).names);
        }

        List<RelNode> phyTableScans = new ArrayList<>();
        for (Map.Entry<String, List<List<String>>> t : targetTables.entrySet()) {
            String group = t.getKey();
            List<List<String>> tableNames = t.getValue();
            for (List<String> subTableNames : tableNames) {
                PhyDdlTableOperation phyDdlTable = new PhyDdlTableOperation(parent);
                phyDdlTable.setDbIndex(group);
                phyDdlTable.setLogicalTableName(logicalTableName);
                if (newTableName != null) {
                    phyDdlTable.setRenameLogicalTableName(((SqlIdentifier) newTableName).getLastName());
                }
                phyDdlTable.setTableNames(ImmutableList.of(subTableNames));
                phyDdlTable.setKind(sqlTemplate.getKind());
                Pair<BytesSql, Map<Integer, ParameterContext>> sqlAndParam = buildSqlAndParam(subTableNames);
                phyDdlTable.setBytesSql(sqlAndParam.getKey());
                phyDdlTable.setNativeSqlNode(sqlTemplate);
                phyDdlTable.setDbType(dbType);
                phyDdlTable.setParam(sqlAndParam.getValue());
                phyDdlTable.setTableRule(tableRule);
                phyDdlTable.setPartitionInfo(partitionInfo);
                phyDdlTable.setPartitioned(isPartitioned);
                phyDdlTable.sequence = sequenceBean;
                phyDdlTable.setHint(isHint);
                phyDdlTable.setSchemaName(this.schemaName);
                phyTableScans.add(phyDdlTable);
            }
        }
        if (!ConfigDataMode.isPolarDbX() && SqlKind.SUPPORT_SHADOW_DDL.contains(sqlTemplate.getKind())) {
            // phy table scan is exists and not hint
            // If phyTableScans.size() == 1, the database has no shards & the
            // table has no shards,
            // in that case, we do not need to create table in shadow database
            if (phyTableScans.size() > 1) {
                if (parent instanceof DataDefLanguageLogicView
                    && ((DataDefLanguageLogicView) parent).getTargetTablesHintCache() == null) {
                    phyTableScans.add(build((PhyDdlTableOperation) phyTableScans.get(0)));
                }
            }
        }
        return phyTableScans;
    }

    public RelNode build(PhyDdlTableOperation phyDdlTableOperation) {
        PhyDdlTableOperation ddlTableOperation = phyDdlTableOperation.copy();

        ddlTableOperation.setShadowDbOnly(true);
        ddlTableOperation.setDbIndex(this.optimizerContext.getRuleManager().getDefaultDbIndex(null));

        List<String> logicalTableNames = new ArrayList<>();
        final String tempL = ddlTableOperation.getLogicalTableName();
        logicalTableNames.add(this.schemaName + "." + tempL);
        if (ddlTableOperation.getNewLogicalTableName() != null) {
            logicalTableNames.add(this.schemaName + "." + ddlTableOperation.getNewLogicalTableName());
        }
        final Map<Integer, ParameterContext> integerParameterContextMap = buildParamsForShadowTable(logicalTableNames);
        ddlTableOperation.setParam(integerParameterContextMap);

        return ddlTableOperation;
    }

    /**
     * build NativeSql and its parameters
     */
    private Pair<BytesSql, Map<Integer, ParameterContext>> buildSqlAndParam(List<String> tableNames) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(tableNames));

        BytesSql sql = BytesSql.getBytesSql(RelUtils.toNativeSql(sqlTemplate, dbType));

        Map<Integer, ParameterContext> params;
        if (sqlTemplate.getKind() == SqlKind.CREATE_TABLE && sqlTemplate instanceof SqlCreateTable &&
            GeneralUtil.isNotEmpty(((SqlCreateTable) sqlTemplate).getLogicalReferencedTables()) &&
            GeneralUtil.isNotEmpty(((SqlCreateTable) sqlTemplate).getPhysicalReferencedTables())) {
            params = PlannerUtils.buildParam(tableNames, ((SqlCreateTable) sqlTemplate).getPhysicalReferencedTables());
        } else if (sqlTemplate.getKind() == SqlKind.ALTER_TABLE && sqlTemplate instanceof SqlAlterTable &&
            GeneralUtil.isNotEmpty(((SqlAlterTable) sqlTemplate).getLogicalReferencedTables()) &&
            GeneralUtil.isNotEmpty(((SqlAlterTable) sqlTemplate).getPhysicalReferencedTables())) {
            params = PlannerUtils.buildParam(tableNames, ((SqlAlterTable) sqlTemplate).getPhysicalReferencedTables());
        } else {
            params = buildParams(tableNames);
        }

        return new Pair<>(sql, params);
    }

    private Map<Integer, ParameterContext> buildParamsForShadowTable(List<String> tableNames) {
        if (sqlTemplate.getKind() == SqlKind.CREATE_TABLE && sqlTemplate instanceof SqlCreateTable &&
            GeneralUtil.isNotEmpty(((SqlCreateTable) sqlTemplate).getLogicalReferencedTables()) &&
            GeneralUtil.isNotEmpty(((SqlCreateTable) sqlTemplate).getPhysicalReferencedTables())) {
            List<String> logicalReferencedTableWithSchema = new ArrayList<>();
            for (String logicalReferencedTable : ((SqlCreateTable) sqlTemplate).getLogicalReferencedTables()) {
                String fullQualifiedTableName = this.schemaName + "." + logicalReferencedTable;
                logicalReferencedTableWithSchema.add(fullQualifiedTableName);
            }
            return PlannerUtils.buildParam(tableNames, logicalReferencedTableWithSchema);
        } else if (sqlTemplate.getKind() == SqlKind.ALTER_TABLE && sqlTemplate instanceof SqlAlterTable &&
            GeneralUtil.isNotEmpty(((SqlAlterTable) sqlTemplate).getLogicalReferencedTables()) &&
            GeneralUtil.isNotEmpty(((SqlAlterTable) sqlTemplate).getPhysicalReferencedTables())) {
            List<String> logicalReferencedTableWithSchema = new ArrayList<>();
            for (String logicalReferencedTable : ((SqlAlterTable) sqlTemplate).getLogicalReferencedTables()) {
                String fullQualifiedTableName = this.schemaName + "." + logicalReferencedTable;
                logicalReferencedTableWithSchema.add(fullQualifiedTableName);
            }
            return PlannerUtils.buildParam(tableNames, logicalReferencedTableWithSchema);
        } else {
            return buildParams(tableNames);
        }
    }

    /**
     * 构建 SQL 对应的参数信息
     */
    private Map<Integer, ParameterContext> buildParams(List<String> tableNames) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(tableNames));

        Map<Integer, ParameterContext> rs = null;
        if (tableNames.size() != paramIndex.size()) {
            List<Integer> paramIndexes = new ArrayList<>();
            for (int i = 0; i < tableNames.size(); i++) {
                paramIndexes.add(-1);
            }
            rs = PlannerUtils.buildParam(tableNames, this.params, paramIndexes);
        } else {
            rs = PlannerUtils.buildParam(tableNames, this.params, paramIndex);
        }

        return rs;
    }

    public String getIndexTableName() {
        return indexTableName;
    }

    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }

    public boolean isDdlOnGsiTable() {
        return ddlOnGsiTable;
    }

    public void setDdlOnGsiTable(boolean ddlOnGsiTable) {
        this.ddlOnGsiTable = ddlOnGsiTable;
    }

    public String getCurrentReplicateTableName() {
        return currentReplicateTableName;
    }

    public void setCurrentReplicateTableName(String currentReplicateTableName) {
        this.currentReplicateTableName = currentReplicateTableName;
    }

    public boolean isAlterTableByAddingPartitions() {
        return alterTableByAddingPartitions;
    }

    public void setAlterTableByAddingPartitions(boolean alterTableByAddingPartitions) {
        this.alterTableByAddingPartitions = alterTableByAddingPartitions;
    }

    public boolean isAlterTableBydroppingPartitions() {
        return alterTableBydroppingPartitions;
    }

    public void setAlterTableBydroppingPartitions(boolean alterTableBydroppingPartitions) {
        this.alterTableBydroppingPartitions = alterTableBydroppingPartitions;
    }
}
