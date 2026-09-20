/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.PolarSecurityLabelColumn;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.lbac.LBACException;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.DefaultExprUtil;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import com.alibaba.polardbx.optimizer.utils.DdlCharsetInfo;
import com.alibaba.polardbx.optimizer.utils.DdlCharsetInfoUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionByRange;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_CREATE_SELECT_FUNCTION_ALIAS;
import static com.alibaba.polardbx.common.properties.ConnectionParams.CREATE_TABLE_WITH_CHARSET_COLLATE;

public class LogicalCreateTable extends LogicalTableOperation {

    protected SqlCreateTable sqlCreateTable;

    protected String createTableSqlForLike;
    protected boolean importTable = false;
    protected boolean reImportTable = false;

    protected CreateTablePreparedData createTablePreparedData;
    private CreateTableWithGsiPreparedData createTableWithGsiPreparedData;
    private SqlSelect sqlSelect;

    boolean ignore = false;
    boolean replace = false;

    protected LogicalCreateTable(CreateTable createTable) {
        super(createTable);
        this.sqlCreateTable = (SqlCreateTable) relDdl.sqlNode;
    }

    public static LogicalCreateTable create(CreateTable createTable) {
        return new LogicalCreateTable(createTable);
    }

    public SqlCreateTable getSqlCreate() {
        return sqlCreateTable;
    }

    public void setSqlSelect(SqlSelect sqlSelect) {
        this.sqlSelect = sqlSelect;
    }

    public SqlSelect getSqlSelect() {
        return sqlSelect;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public void setReplace(boolean replace) {
        this.replace = replace;
    }

    public boolean isReplace() {
        return replace;
    }

    public boolean isWithGsi() {
        return createTableWithGsiPreparedData != null
            && createTableWithGsiPreparedData.hasGsi()
            && !Engine.isFileStore(sqlCreateTable.getEngine()); // no gsi for oss table
    }

    public boolean isBroadCastTable() {
        return sqlCreateTable.isBroadCast();
    }

    public boolean isReplicasTable() {
        return sqlCreateTable.isReplicas();
    }

    public boolean isPartitionTable() {
        return sqlCreateTable.getSqlPartition() != null;
    }

    public void setCreateTableSqlForLike(String createTableSqlForLike) {
        this.createTableSqlForLike = createTableSqlForLike;
    }

    public String getCreateTableSqlForLike() {
        return this.createTableSqlForLike;
    }

    public CreateTablePreparedData getCreateTablePreparedData() {
        return createTablePreparedData;
    }

    public CreateTableWithGsiPreparedData getCreateTableWithGsiPreparedData() {
        return createTableWithGsiPreparedData;
    }

    public SqlCreateTable getSqlCreateTable() {
        return sqlCreateTable;
    }

    public boolean isImportTable() {
        return importTable;
    }

    public void setImportTable(boolean importTable) {
        this.importTable = importTable;
    }

    public boolean isReImportTable() {
        return reImportTable;
    }

    public void setReImportTable(boolean reImportTable) {
        this.reImportTable = reImportTable;
    }

    public void prepareData(ExecutionContext executionContext) {
        // A normal logical table or a primary table with GSIs.
        createTablePreparedData = preparePrimaryData(executionContext);

        createTablePreparedData.setImportTable(this.importTable);

        createTablePreparedData.setReimportTable(this.reImportTable);

        final boolean isAutoPartition = sqlCreateTable.isAutoPartition();

        if (Engine.isFileStore(sqlCreateTable.getEngine())) {
            // no gsi for oss table
            return;
        }

        // Init primary table definition
        if (sqlCreateTable.createGsiOrCci()) {
            final String primaryTableName = createTablePreparedData.getTableName();
            final String primaryTableDefinition = sqlCreateTable.rewriteForGsi().toString();

            createTablePreparedData.setTableDefinition(primaryTableDefinition);

            createTableWithGsiPreparedData = new CreateTableWithGsiPreparedData();
            createTableWithGsiPreparedData.setPrimaryTablePreparedData(createTablePreparedData);

            if (sqlCreateTable.getGlobalKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> gsi : sqlCreateTable.getGlobalKeys()) {
                    CreateGlobalIndexPreparedData indexTablePreparedData =
                        prepareGsiData(primaryTableName, primaryTableDefinition, gsi, false);
                    createTableWithGsiPreparedData.addIndexTablePreparedData(indexTablePreparedData);
                    indexTablePreparedData.setJoinGroupName(createTablePreparedData.getJoinGroupName());
                    indexTablePreparedData.setVisible(gsi.getValue().isVisible());
                    if (isAutoPartition) {
                        createTableWithGsiPreparedData.addLocalIndex(
                            prepareAutoPartitionLocalIndex(primaryTableName, gsi));
                    }
                }
            }

            if (sqlCreateTable.getGlobalUniqueKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> gusi : sqlCreateTable.getGlobalUniqueKeys()) {
                    CreateGlobalIndexPreparedData uniqueIndexTablePreparedData =
                        prepareGsiData(primaryTableName, primaryTableDefinition, gusi, true);
                    uniqueIndexTablePreparedData.setVisible(gusi.getValue().isVisible());
                    uniqueIndexTablePreparedData.setJoinGroupName(createTablePreparedData.getJoinGroupName());
                    createTableWithGsiPreparedData.addIndexTablePreparedData(uniqueIndexTablePreparedData);
                }
            }

            if (sqlCreateTable.getClusteredKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> gsi : sqlCreateTable.getClusteredKeys()) {
                    CreateGlobalIndexPreparedData indexTablePreparedData =
                        prepareGsiData(primaryTableName, primaryTableDefinition, gsi, false);
                    indexTablePreparedData.setVisible(gsi.getValue().isVisible());
                    indexTablePreparedData.setJoinGroupName(createTablePreparedData.getJoinGroupName());
                    createTableWithGsiPreparedData.addIndexTablePreparedData(indexTablePreparedData);

                    if (isAutoPartition) {
                        createTableWithGsiPreparedData.addLocalIndex(
                            prepareAutoPartitionLocalIndex(primaryTableName, gsi));
                    }
                }
            }

            if (sqlCreateTable.getClusteredUniqueKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> gusi : sqlCreateTable.getClusteredUniqueKeys()) {
                    CreateGlobalIndexPreparedData uniqueIndexTablePreparedData =
                        prepareGsiData(primaryTableName, primaryTableDefinition, gusi, true);
                    uniqueIndexTablePreparedData.setVisible(gusi.getValue().isVisible());
                    uniqueIndexTablePreparedData.setJoinGroupName(createTablePreparedData.getJoinGroupName());
                    createTableWithGsiPreparedData.addIndexTablePreparedData(uniqueIndexTablePreparedData);
                }
            }

            if (sqlCreateTable.getColumnarKeys() != null) {
                for (Pair<SqlIdentifier, SqlIndexDefinition> cci : sqlCreateTable.getColumnarKeys()) {
                    // Use CreateGlobalIndexPreparedData#columnarIndex for marking cci
                    CreateGlobalIndexPreparedData columnarIndexTablePreparedData =
                        prepareGsiData(primaryTableName, primaryTableDefinition, cci, false);
                    columnarIndexTablePreparedData.setVisible(cci.getValue().isVisible());
                    columnarIndexTablePreparedData.setJoinGroupName(createTablePreparedData.getJoinGroupName());
                    columnarIndexTablePreparedData.setCreateTableWithIndex(true);
                    createTableWithGsiPreparedData.addIndexTablePreparedData(columnarIndexTablePreparedData);
                }
            }
        }
    }

    String generateInsert(ExecutionContext executionContext) {
        // executionContext.getSchemaManager().getTable().
        // selectSql = prepareSelectSql();
        // 设置完 selectSql，然后set一下
        // insert into table(+属性) select ** from
        // ignore 和 replace 和 排序
        SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
        writer.setAlwaysUseParentheses(true);
        writer.setSelectListItemsOnSeparateLines(false);
        writer.setIndentation(0);
        if (replace == true) {
            writer.keyword("REPLACE");
        } else {
            writer.keyword("INSERT");
            if (ignore == true) {
                writer.keyword("IGNORE");
            }
        }

        writer.keyword("INTO");
        if (replace == false) {
            writer.keyword("TABLE");
        }
        sqlCreateTable.getName().unparse(writer, 0, 0);
        SqlWriter.Frame frame = writer.startList("(", ")");
        // 插入select的属性
        for (SqlNode c : sqlSelect.getSelectList()) {
            writer.sep(",");
            if (c instanceof SqlIdentifier) {
                SqlIdentifier col = (SqlIdentifier) c;
                writer.identifier(col.getLastName());
            } else if (c instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) c;
                if (basicCall.getOperator().getKind().equals(SqlKind.AS)) {
                    if (basicCall.getOperands()[0].toString()
                        .equalsIgnoreCase(basicCall.getOperands()[basicCall.getOperands().length - 1].toString())) {
                        throw new TddlRuntimeException(ERR_CREATE_SELECT_FUNCTION_ALIAS,
                            "must alias all function calls or expressions in the query");
                    }
                    basicCall.getOperands()[basicCall.getOperands().length - 1].unparse(writer, 0, 0);
                } else {
                    throw new TddlRuntimeException(ERR_CREATE_SELECT_FUNCTION_ALIAS,
                        "must alias all function calls or expressions in the query");
                }
            } else if (c instanceof SqlLiteral) {
                // Change context:
                // - Before: unaliased SqlLiteral select items matched neither branch above and were
                //   silently skipped (the generic c.unparse fallback below was disabled long ago,
                //   historical rationale not confirmed), leaving the backfill INSERT column list
                //   empty/misaligned; InsertIntoTask validation then reported the misleading
                //   "Unknown target column '<table name>'" and the DDL job rolled back.
                // - Path impact: only affects CTAS INSERT generation for unaliased literal items;
                //   bare identifiers, AS-aliased items and unaliased expressions keep their
                //   existing behavior, and non-CTAS paths never call generateInsert.
                // - Capability regression: unaliased-literal CTAS now fails fast at plan time with
                //   an explicit alias-required error instead of reaching InsertIntoTask; this is
                //   consistent with the existing rejection of unaliased expressions and users can
                //   recover by adding an AS alias.
                throw new TddlRuntimeException(ERR_CREATE_SELECT_FUNCTION_ALIAS,
                    "must alias all literals, function calls or expressions in the query");
            }
            // c.unparse(writer, 0, 0);
        }
        writer.endList(frame);
        sqlSelect.unparse(writer, 0, 0);

//        if (sqlCreateTable.getPrimaryKey() == null) {
//            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
//                String.format("Primary key is null."));
//        }
//        writer.keyword("ORDER");
//        writer.keyword("BY");
//        sqlCreateTable.getPrimaryKey().getColumns().get(0).unparse(writer, 0, 0);
//        if (sqlSelect.hasOrderBy() == false) {
//            SqlNode fromTable = sqlSelect.getFrom();
//            if (fromTable instanceof SqlIdentifier && ((SqlIdentifier) fromTable).names.size() > 1) {
//                // List<String> names = ((SqlIdentifier) fromTable).names;
//                writer.keyword("ORDER");
//                writer.keyword("BY");
//                SqlIdentifier table = (SqlIdentifier) fromTable;
//                executionContext.getSchemaManager().getTable(table.getLastName()).getPrimaryKey()
//            }
//        }
        return writer.toSqlString().getSql();
    }

    private CreateTablePreparedData preparePrimaryData(ExecutionContext executionContext) {
        if (sqlCreateTable.getLikeTableName() != null) {
            prepareCreateTableLikeData();
        }
        String selectSql = null;
        if (sqlCreateTable.isSelect()) {
            // CTAS silently loses externalized columns: the dest column type collapses
            // to varchar(0) and content goes empty. Block until CTAS understands ext cols.
            rejectCtasOnExternalizedSource(executionContext);
            selectSql = generateInsert(executionContext);
        }
        // for mysql8.0
        MySqlCreateTableStatement stmt =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(sqlCreateTable.getSourceSql()).get(0);

        Set<String> requestedExternalizedColumns = collectExternalizedColumnNames(stmt.getColumnDefinitions());
        boolean hasExternalizedColumns = !requestedExternalizedColumns.isEmpty();
        boolean isDrds = !DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        if (hasExternalizedColumns && isDrds) {
            validateNoDrdsShardKeyOnExternalizedColumn(requestedExternalizedColumns);
        }

        // Transform externalized columns: replace EXTERNALIZE columns with physical addr columns
        rewriteExternalizedColumns(stmt);

        if (hasExternalizedColumns && sqlCreateTable.isBroadCast()) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "EXTERNALIZE column on BROADCAST table");
        }

        boolean useDbCharset = executionContext.getParamManager().getBoolean(CREATE_TABLE_WITH_CHARSET_COLLATE);
        String defaultCharset = sqlCreateTable.getDefaultCharset();
        String defaultCollation = sqlCreateTable.getDefaultCollation();
        if (useDbCharset) {
            String charset;
            String collation;
            StringBuilder builder = new StringBuilder();
            charset = DbInfoManager.getInstance().getDbChartSet(schemaName);
            collation = DbInfoManager.getInstance().getDbCollation(schemaName);

            if (StringUtils.isEmpty(charset) || StringUtils.isEmpty(collation)) {
                /**
                 * For some unit-test, its dbInfo is mock,so its charset & collation maybe null
                 */
                // Fetch server default collation
                DdlCharsetInfo serverDefaultCharsetInfo =
                    DdlCharsetInfoUtil.fetchServerDefaultCharsetInfo(executionContext, true);

                // Fetch db collation by charset & charset defined by user and server default collation
                DdlCharsetInfo createDbCharInfo =
                    DdlCharsetInfoUtil.decideDdlCharsetInfo(executionContext, serverDefaultCharsetInfo.finalCharset,
                        serverDefaultCharsetInfo.finalCollate,
                        charset, collation, true);
                charset = createDbCharInfo.finalCharset;
                collation = createDbCharInfo.finalCollate;
            }

            // Fetch tbl collation by charset & charset defined by user and db collation
            DdlCharsetInfo createTbCharInfo =
                DdlCharsetInfoUtil.decideDdlCharsetInfo(executionContext, charset, collation,
                    defaultCharset, defaultCollation, true);

            if (defaultCharset == null && defaultCollation == null && createTbCharInfo.finalCharset != null) {
                builder.append(" CHARSET ").append(createTbCharInfo.finalCharset.toLowerCase());
                sqlCreateTable.setDefaultCharset(createTbCharInfo.finalCharset.toLowerCase());
            }

            if (defaultCollation == null && createTbCharInfo.finalCollate != null) {
                builder.append(" COLLATE ").append(createTbCharInfo.finalCollate.toLowerCase());
                sqlCreateTable.setDefaultCollation(createTbCharInfo.finalCollate.toLowerCase());
            }

            stmt.setAfterSemi(false);
            if (sqlCreateTable.getQuery() == null) {
                sqlCreateTable.setSourceSql(stmt.toString() + builder);
            }
        } else if (hasExternalizedColumns && sqlCreateTable.getQuery() == null) {
            // When useDbCharset=false, the original code doesn't update sourceSql.
            // But externalized columns have been physicalized in stmt (EXTERNALIZE -> addr columns),
            // so we must write the physicalized DDL back; otherwise DN receives EXTERNALIZE keywords.
            stmt.setAfterSemi(false);
            sqlCreateTable.setSourceSql(stmt.toString());
        }

        String tableName = ((SqlIdentifier) sqlCreateTable.getName()).getLastName();
        final TableMeta tableMeta = TableMetaParser.parse(tableName, sqlCreateTable);
        tableMeta.setSchemaName(schemaName);

        LocalPartitionDefinitionInfo localPartitionDefinitionInfo = LocalPartitionDefinitionInfo.create(
            schemaName,
            tableMeta.getTableName(),
            (SqlPartitionByRange) sqlCreateTable.getLocalPartition()
        );

        TtlDefinitionInfo ttlDefinitionInfo = null;
        SqlNode ttlDefinitionExpr = null;
        if (localPartitionDefinitionInfo != null) {
            SQLPartitionByRange sqlPartitionByRange = LocalPartitionDefinitionInfo.generateLocalPartitionStmtForCreate(
                localPartitionDefinitionInfo,
                localPartitionDefinitionInfo.evalPivotDate(executionContext));
            sqlCreateTable.setLocalPartitionSuffix(sqlPartitionByRange);
        } else {
            if (sqlCreateTable.getTtlDefinition() != null) {
                ttlDefinitionExpr = sqlCreateTable.getTtlDefinition();
            }
        }

        CreateTablePreparedData res = prepareCreateTableData(tableMeta,
            sqlCreateTable.isShadow(),
            sqlCreateTable.isAutoPartition(),
            sqlCreateTable.isBroadCast(),
            sqlCreateTable.getDbpartitionBy(),
            sqlCreateTable.getDbpartitions(),
            sqlCreateTable.getTbpartitionBy(),
            sqlCreateTable.getTbpartitions(),
            sqlCreateTable.getSqlPartition(),
            localPartitionDefinitionInfo,
            ttlDefinitionInfo,
            ttlDefinitionExpr,
            sqlCreateTable.getTableGroupName(),
            sqlCreateTable.isWithImplicitTableGroup(),
            sqlCreateTable.getJoinGroupName(),
            sqlCreateTable.getLocality(),
            ((CreateTable) relDdl).getPartBoundExprInfo(),
            sqlCreateTable.getOriginalSql(),
            sqlCreateTable.getLogicalReferencedTables(),
            sqlCreateTable.getAddedForeignKeys());
        res.setSelectSql(selectSql);

        boolean hasTimestampColumnDefault = false;
        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(sqlCreateTable.getColDefs())) {
            if (isTimestampColumnWithDefault(colDef.getValue())) {
                hasTimestampColumnDefault = true;
                break;
            }
        }
        res.setTimestampColumnDefault(hasTimestampColumnDefault);

        Map<String, String> specialDefaultValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Long> specialDefaultValueFlags = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Set<String>> genColRefs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> allReferencedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<String> allColumns = new ArrayList<>();
        Set<String> autoIncColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> autoUpdateColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> mysqlGenColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> externalizedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(sqlCreateTable.getColDefs())) {
            String columnName = colDef.getKey().getLastName();
            allColumns.add(columnName);

            if (colDef.getValue().isAutoIncrement()) {
                autoIncColumns.add(columnName);
            }
            if (colDef.getValue().isOnUpdateCurrentTimestamp()) {
                autoUpdateColumns.add(columnName);
            }
            if (colDef.getValue().isGeneratedAlways() && !colDef.getValue().isGeneratedAlwaysLogical()) {
                mysqlGenColumns.add(columnName);
            }

            if (colDef.getValue().isGeneratedAlwaysLogical()) {
                if (colDef.getValue().isAutoIncrement()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Generated column [%s] can not be auto_increment column.", colDef.getKey()));
                }
                if (colDef.getValue().isOnUpdateCurrentTimestamp()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Generated column [%s] can not be auto update column.", colDef.getKey()));
                }
                if (!GeneratedColumnUtil.supportDataType(colDef.getValue())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Do not support data type of generated column [%s].", colDef.getKey()));
                }

                SqlCall expr = colDef.getValue().getGeneratedAlwaysExpr();
                GeneratedColumnUtil.validateGeneratedColumnExpr(expr);
                genColRefs.put(columnName, GeneratedColumnUtil.getReferencedColumns(expr));
                allReferencedColumns.addAll(genColRefs.get(columnName));
                specialDefaultValues.put(columnName, expr.toString());
                specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN);
            } else if (colDef.getValue().isGeneratedAlways()) {
                SqlCall expr = colDef.getValue().getGeneratedAlwaysExpr();
                GeneratedColumnUtil.validateGeneratedColumnExpr(expr);
                specialDefaultValues.put(columnName, expr.toString());
                specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_GENERATED_COLUMN);
            } else if (colDef.getValue().getDefaultVal() instanceof SqlBinaryStringLiteral) {
                String hexValue =
                    ((SqlBinaryStringLiteral) colDef.getValue().getDefaultVal()).getBitString().toHexString();
                specialDefaultValues.put(columnName, hexValue);
                specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
            } else if (colDef.getValue().getDefaultExpr() != null && InstanceVersion.isMYSQL80()) {
                if (!DefaultExprUtil.supportDataType(colDef.getValue())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Do not support data type of column `%s` with default expression.",
                            colDef.getKey()));
                }
                SqlCall expr = colDef.getValue().getDefaultExpr();
                DefaultExprUtil.validateColumnExpr(expr);
                specialDefaultValues.put(columnName, expr.toString());
                specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_DEFAULT_EXPR);
            }

            // Track externalized columns — use the physical addr column name for MetaDB flag
            if (colDef.getValue().isExternalize()) {
                externalizedColumns.add(columnName);
                String addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
                specialDefaultValues.put(addrColumnName, "''");
                specialDefaultValueFlags.put(addrColumnName, ColumnsRecord.FLAG_EXTERNALIZED_COLUMN);
            }
        }

        for (Map.Entry<String, Set<String>> generatedColumn : genColRefs.entrySet()) {
            for (String referencedColumn : generatedColumn.getValue()) {
                if (externalizedColumns.contains(referencedColumn)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Cannot create generated column [%s] referencing externalized column [%s].",
                            generatedColumn.getKey(), referencedColumn));
                }
            }
        }

        for (String referencedColumn : allReferencedColumns) {
            if (allColumns.stream().noneMatch(c -> c.equalsIgnoreCase(referencedColumn))) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Referenced column [%s] does not exist", referencedColumn));
            } else if (autoIncColumns.contains(referencedColumn)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Referenced column [%s] can not be auto_increment column.", referencedColumn));
            } else if (autoUpdateColumns.contains(referencedColumn)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Referenced column [%s] can not be auto update column.", referencedColumn));
            } else if (mysqlGenColumns.contains(referencedColumn)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Referenced column [%s] can not be non-logical generated column.", referencedColumn));
            }
        }

        for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : GeneralUtil.emptyIfNull(sqlCreateTable.getColDefs())) {
            String columnName = colDef.getKey().getLastName();
            if (allReferencedColumns.contains(columnName)) {
                if (!GeneratedColumnUtil.supportDataType(colDef.getValue())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Do not support data type of referenced column [%s].", colDef.getKey()));
                }
            }
        }

        // Check cycle
        GeneratedColumnUtil.getGeneratedColumnEvaluationOrder(genColRefs);

        res.setSpecialDefaultValues(specialDefaultValues);
        res.setSpecialDefaultValueFlags(specialDefaultValueFlags);

        // create table with locality
        LocalityDesc desc = LocalityDesc.parse(sqlCreateTable.getLocality(), schemaName);
        res.setLocality(desc);

        /**
         * Check if the loading-source table of archive Table is using
         * local-partition or row-level-ttl
         */
        boolean archiveFromRowLevelTtlTbl = false;

        if (sqlCreateTable.getArchiveMode() == ArchiveMode.TTL) {
            String srcTblSchema = sqlCreateTable.getLoadTableSchema();
            if (srcTblSchema == null) {
                srcTblSchema = schemaName;
            }
            String srcTblName = sqlCreateTable.getLoadTableName();
            archiveFromRowLevelTtlTbl = TtlUtil.checkIfUsingRowLevelTtl(srcTblSchema, srcTblName, executionContext);
        }

        if (!archiveFromRowLevelTtlTbl) {
            /**
             * Archive from local-partition table
             */
            if (sqlCreateTable.getLoadTableName() != null) {
                res.setLoadTableName(sqlCreateTable.getLoadTableName());
                if (sqlCreateTable.getLoadTableSchema() != null) {
                    res.setLoadTableSchema(sqlCreateTable.getLoadTableSchema());
                } else {
                    res.setLoadTableSchema(schemaName);
                }
            }

            if (sqlCreateTable.getArchiveMode() == ArchiveMode.TTL) {
                res.setArchiveTmpTableName(sqlCreateTable.getLoadTableName());
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, "Not support create oss table like a ttl-defined table");
        }

        if (res.isWithImplicitTableGroup()) {
            TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(res.getSchemaName()).getTableGroupInfoManager();
            String tableGroupName = res.getTableGroupName() == null ? null :
                ((SqlIdentifier) res.getTableGroupName()).getLastName();
            assert tableGroupName != null;
            if (tableGroupInfoManager.getTableGroupConfigByName(tableGroupName) == null) {
                res.getRelatedTableGroupInfo().put(tableGroupName, true);
            } else {
                res.getRelatedTableGroupInfo().put(tableGroupName, false);
            }
        }

        //check and collect lbac attr
        if (sqlCreateTable.getSecurityPolicy() != null) {
            if (LBACSecurityManager.getInstance().getPolicy(sqlCreateTable.getSecurityPolicy()) == null) {
                throw new LBACException("security policy is not exist");
            }
            LBACSecurityEntity esa =
                new LBACSecurityEntity(LBACSecurityEntity.EntityKey.createTableKey(schemaName, tableName),
                    LBACSecurityEntity.EntityType.TABLE, sqlCreateTable.getSecurityPolicy());
            res.setTableEAS(esa);
        }

        for (Pair<SqlIdentifier, SqlColumnDeclaration> pair : sqlCreateTable.getColDefs()) {
            if (pair.getValue().getSecuredWith() != null) {
                List<LBACSecurityEntity> colESAList = res.getColEASList();
                if (colESAList == null) {
                    colESAList = new ArrayList<>();
                    res.setColEASList(colESAList);
                }
                colESAList.add(new LBACSecurityEntity(
                    LBACSecurityEntity.EntityKey.createColumnKey(schemaName, tableName, pair.getKey().getSimple()),
                    LBACSecurityEntity.EntityType.COLUMN,
                    pair.getValue().getSecuredWith()
                ));
            }
            //check psl column type
            if (PolarSecurityLabelColumn.checkPSLName(pair.getKey().getSimple()) &&
                !PolarSecurityLabelColumn.checkPSLType(pair.getValue().getDataType().getTypeName().getSimple())) {
                throw new LBACException("polar security column type is invalid");
            }
        }

        return res;
    }

    private void prepareCreateTableLikeData() {
        // For `create table like xx` statement, we create a new "Create Table" AST for the target table
        // based on the LIKE table, then execute it as normal flow.
        SqlCreateTable createTableAst =
            (SqlCreateTable) new FastsqlParser()
                .parse(createTableSqlForLike, PlannerContext.getPlannerContext(this).getExecutionContext()).get(0);

        SqlIdentifier tableName = (SqlIdentifier) getTableNameNode();

        createTableAst.setTargetTable(tableName);

        if (createTableAst.getAutoIncrement() != null) {
            createTableAst.getAutoIncrement().setStart(null);
        }

        createTableAst.setMappingRules(null);
        createTableAst.setGlobalKeys(null);
        createTableAst.setGlobalUniqueKeys(null);

        // set engine if there is engine option in user sql
        Engine engine;
        if ((engine = this.sqlCreateTable.getEngine()) != null) {
            createTableAst.setEngine(engine);

            createTableAst.setTtlEnable(null);
            createTableAst.setTtlExpr(null);
            createTableAst.setTtlJob(null);
            createTableAst.setTtlDefinition(null);

        }

        ArchiveMode archiveMode;
        if ((archiveMode = this.sqlCreateTable.getArchiveMode()) != null) {
            createTableAst.setArchiveMode(archiveMode);
        }

        List<String> dictColumns;
        if ((dictColumns = sqlCreateTable.getDictColumns()) != null) {
            createTableAst.setDictColumns(dictColumns);
        }

        if (this.sqlCreateTable.shouldLoad() || this.sqlCreateTable.shouldBind()) {
            if (sqlCreateTable.getLikeTableName() instanceof SqlIdentifier) {
                if (((SqlIdentifier) sqlCreateTable.getLikeTableName()).names.size() == 2) {
                    createTableAst.setLoadTableSchema(
                        ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(0).getLastName());
                    createTableAst.setLoadTableName(
                        ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(1).getLastName());
                } else if (((SqlIdentifier) sqlCreateTable.getLikeTableName()).names.size() == 1) {
                    createTableAst.setLoadTableName(
                        ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(0).getLastName());
                }
            }
        }

        // Replace the original AST
        this.sqlCreateTable = createTableAst;
        this.relDdl.sqlNode = createTableAst;
    }

    private CreateGlobalIndexPreparedData prepareGsiData(String primaryTableName, String primaryTableDefinition,
                                                         Pair<SqlIdentifier, SqlIndexDefinition> gsi,
                                                         boolean isUnique) {

        String indexTableName = RelUtils.lastStringValue(gsi.getKey());
        SqlIndexDefinition indexDef = gsi.getValue();

        /**
         * If the primary table is auto-partition, the global index of this table
         * is also auto-partitioned
         */
        boolean autoPartitionGsi = createTablePreparedData.isAutoPartition();

        CreateGlobalIndexPreparedData preparedData =
            prepareCreateGlobalIndexData(primaryTableName,
                primaryTableDefinition,
                indexTableName,
                createTablePreparedData.getTableMeta(),
                createTablePreparedData.isShadow(),
                autoPartitionGsi,
                false,
                indexDef.getDbPartitionBy(),
                indexDef.getDbPartitions(),
                indexDef.getTbPartitionBy(),
                indexDef.getTbPartitions(),
                indexDef.getPartitioning(),
                createTablePreparedData.getLocalPartitionDefinitionInfo(),
                isUnique,
                indexDef.isClustered(),
                indexDef.isColumnar(),
                indexDef.getTableGroupName(),
                indexDef.isWithImplicitTableGroup(),
                indexDef.getEngineName(),
                // Do not set locality for cci
                indexDef.isColumnar() ? "" : createTablePreparedData.getLocality().toString(),
                ((CreateTable) relDdl).getPartBoundExprInfo(),
                createTablePreparedData.getSourceSql());

        // Propagate externalized column flags from primary table to inline GSI.
        // The primaryTableMeta here is parsed from AST (not loaded from MetaDB),
        // so columnMeta.isExternalizedColumn() is always false. We must copy the
        // flags from the primary table's specialDefaultValueFlags instead.
        Map<String, Long> primaryFlags = createTablePreparedData.getSpecialDefaultValueFlags();
        Map<String, String> primaryDefaults = createTablePreparedData.getSpecialDefaultValues();
        if (primaryFlags != null) {
            CreateTablePreparedData indexTablePreparedData = preparedData.getIndexTablePreparedData();
            for (Map.Entry<String, Long> entry : primaryFlags.entrySet()) {
                if (entry.getValue() == ColumnsRecord.FLAG_EXTERNALIZED_COLUMN) {
                    indexTablePreparedData.getSpecialDefaultValues().put(entry.getKey(),
                        primaryDefaults.get(entry.getKey()));
                    indexTablePreparedData.getSpecialDefaultValueFlags().put(entry.getKey(), entry.getValue());
                }
            }
        }

        preparedData.setIndexDefinition(indexDef);
        if (indexDef.getOptions() != null) {
            final String indexComment = indexDef.getOptions()
                .stream()
                .filter(option -> null != option.getComment())
                .findFirst()
                .map(option -> RelUtils.stringValue(option.getComment()))
                .orElse("");
            preparedData.setIndexComment(indexComment);
        }
        if (indexDef.getIndexType() != null) {
            preparedData.setIndexType(null == indexDef.getIndexType() ? null : indexDef.getIndexType().name());
        }
        if (preparedData.isWithImplicitTableGroup()) {
            TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager();
            String tableGroupName = preparedData.getTableGroupName() == null ? null :
                ((SqlIdentifier) preparedData.getTableGroupName()).getLastName();
            assert tableGroupName != null;
            if (tableGroupInfoManager.getTableGroupConfigByName(tableGroupName) == null) {
                createTablePreparedData.getRelatedTableGroupInfo().put(tableGroupName, true);
            } else {
                createTablePreparedData.getRelatedTableGroupInfo().put(tableGroupName, false);
            }
        }

        return preparedData;
    }

    private CreateLocalIndexPreparedData prepareAutoPartitionLocalIndex(String tableName,
                                                                        Pair<SqlIdentifier, SqlIndexDefinition> gsi) {
        String indexName = RelUtils.lastStringValue(gsi.getKey());
        boolean isOnClustered = gsi.getValue().isClustered();

        return prepareCreateLocalIndexData(tableName, indexName, isOnClustered, true);
    }

    public void setDdlVersionId(Long ddlVersionId) {
        if (null != getCreateTablePreparedData()) {
            getCreateTablePreparedData().setDdlVersionId(ddlVersionId);
        }
        if (null != getCreateTableWithGsiPreparedData()) {
            getCreateTableWithGsiPreparedData().setDdlVersionId(ddlVersionId);
        }
    }

    /**
     * Block CREATE TABLE AS SELECT (CTAS) whose SELECT references any table that has
     * externalized columns. Without this, the dest table column type collapses to
     * varchar(0) and content is silently lost.
     */
    private void rejectCtasOnExternalizedSource(ExecutionContext executionContext) {
        // The SELECT used by CTAS is set on `this.sqlSelect` (not sqlCreateTable.getQuery()).
        if (sqlSelect == null) {
            return;
        }
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        Set<String> seen = new java.util.HashSet<>();
        collectSourceTableNames(sqlSelect, seen);
        for (String t : seen) {
            TableMeta tm = sm.getTableWithNull(t);
            if (tm != null && tm.hasExternalizedColumn()) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "CREATE TABLE AS SELECT is not supported when the source table '" + t
                        + "' contains externalized columns. "
                        + "Use INSERT INTO new_table SELECT ... with an explicit schema instead.");
            }
        }
    }

    /**
     * Walk a SqlNode tree collecting identifiers that appear in FROM clauses
     * (the only place a real table reference can sit). Schema-qualified names get
     * stripped to the table portion since SchemaManager lookups go by table name.
     */
    private static void collectSourceTableNames(SqlNode node, Set<String> out) {
        if (node == null) {
            return;
        }
        if (node instanceof SqlSelect) {
            SqlSelect sel = (SqlSelect) node;
            collectFromForTables(sel.getFrom(), out);
            collectSourceTableNames(sel.getWhere(), out);
            collectSourceTableNames(sel.getHaving(), out);
        } else if (node instanceof SqlBasicCall) {
            for (SqlNode op : ((SqlBasicCall) node).getOperandList()) {
                collectSourceTableNames(op, out);
            }
        } else if (node instanceof SqlNodeList) {
            for (SqlNode child : (SqlNodeList) node) {
                collectSourceTableNames(child, out);
            }
        }
    }

    private static void collectFromForTables(SqlNode from, Set<String> out) {
        if (from == null) {
            return;
        }
        if (from instanceof SqlIdentifier) {
            String name = ((SqlIdentifier) from).getLastName();
            if (name != null && !name.isEmpty()) {
                out.add(name);
            }
        } else if (from instanceof SqlJoin) {
            SqlJoin j = (SqlJoin) from;
            collectFromForTables(j.getLeft(), out);
            collectFromForTables(j.getRight(), out);
        } else if (from instanceof SqlBasicCall) {
            // AS, table function, etc — recurse into operands so identifiers underneath are caught
            SqlBasicCall bc = (SqlBasicCall) from;
            for (SqlNode op : bc.getOperandList()) {
                collectFromForTables(op, out);
            }
            // also catch nested SELECT in the source
            for (SqlNode op : bc.getOperandList()) {
                collectSourceTableNames(op, out);
            }
        } else if (from instanceof SqlSelect) {
            // sub-select in FROM
            collectSourceTableNames(from, out);
        }
    }

    /**
     * Rewrite externalized columns in the physical CREATE TABLE statement.
     * Transforms: col LONGTEXT EXTERNALIZE
     * Into:       col_addr_ VARCHAR(128) COMMENT 'ext_type:LONGTEXT'
     */
    private boolean rewriteExternalizedColumns(MySqlCreateTableStatement stmt) {
        Set<String> externalizedCols = rewriteExternalizedColumnDefs(stmt.getColumnDefinitions());

        // Validate no inline indexes (INDEX/KEY/UNIQUE) reference externalized columns.
        // CCI (columnar indexes) are excluded since they are required for blob mapping.
        if (!externalizedCols.isEmpty()) {
            checkNoIndexOnExternalizedColumns(stmt, externalizedCols);
        }
        return !externalizedCols.isEmpty();
    }

    private Set<String> collectExternalizedColumnNames(Iterable<SQLColumnDefinition> columnDefs) {
        Set<String> externalizedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (SQLColumnDefinition columnDef : columnDefs) {
            if (columnDef.isExternalize()) {
                externalizedColumns.add(SQLUtils.normalizeNoTrim(columnDef.getColumnName()));
            }
        }
        return externalizedColumns;
    }

    private void validateNoDrdsShardKeyOnExternalizedColumn(Set<String> externalizedColumns) {
        Set<String> shardColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        SqlCreateTable.getShardingKeys(sqlCreateTable.getDbpartitionBy(), shardColumns, false);
        SqlCreateTable.getShardingKeys(sqlCreateTable.getTbpartitionBy(), shardColumns, false);
        shardColumns.retainAll(externalizedColumns);
        if (!shardColumns.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "EXTERNALIZE column cannot be a DBPARTITION or TBPARTITION key: "
                    + String.join(", ", shardColumns));
        }
    }

    /**
     * Validate an EXTERNALIZE column definition without mutating its AST.
     *
     * @return the normalized original SQL type name
     */
    public static String validateExternalizedColumnDef(SQLColumnDefinition colDef) {
        if (colDef == null || colDef.getDataType() == null || colDef.getDataType().getName() == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "EXTERNALIZE column must declare a data type");
        }

        String originalName = colDef.getColumnName();
        if (colDef.getGeneratedAlawsAs() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                String.format("Generated column [%s] cannot be EXTERNALIZE.", originalName));
        }
        String originalType = colDef.getDataType().getName().toUpperCase();
        if (!ExternalizedColumnInfo.isSupportedType(originalType)) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                String.format("Column [%s] with type [%s] does not support EXTERNALIZE. "
                        + "Only TEXT/BLOB family types are supported.",
                    originalName, originalType));
        }

        boolean isTextFamily = !originalType.endsWith("BLOB");
        if (isTextFamily) {
            String userCharset = null;
            if (colDef.getDataType() instanceof com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType) {
                userCharset = ((com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType)
                    colDef.getDataType()).getCharSetName();
            }
            if (userCharset == null && colDef.getCharsetExpr() != null) {
                userCharset = SQLUtils.normalizeNoTrim(colDef.getCharsetExpr().toString());
            }
            if (!ExternalizedColumnInfo.isAcceptableCharset(userCharset)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Externalized TEXT column [%s] only supports utf8/utf8mb3/utf8mb4 charset, "
                            + "got [%s]. Use LONGBLOB if you need raw bytes in a different encoding.",
                        originalName, userCharset));
            }
        }

        if (colDef.getDefaultExpr() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                String.format("DEFAULT value is not supported on EXTERNALIZE column [%s].",
                    originalName));
        }
        return originalType;
    }

    /**
     * Rewrite an iterable of column definitions in place: any def with isExternalize()
     * becomes its physical addr representation (VARCHAR(128) + ext_type comment).
     * Returns the set of original (logical) column names that were rewritten.
     * <p>Used by both CREATE TABLE and ALTER TABLE ADD COLUMN paths.
     */
    public static Set<String> rewriteExternalizedColumnDefs(Iterable<SQLColumnDefinition> columnDefs) {
        Set<String> externalizedCols = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (SQLColumnDefinition colDef : columnDefs) {
            if (!colDef.isExternalize()) {
                continue;
            }

            String originalName = colDef.getColumnName();
            String originalType = validateExternalizedColumnDef(colDef);

            externalizedCols.add(SQLUtils.normalizeNoTrim(originalName));

            String cleanName = SQLUtils.normalizeNoTrim(originalName);
            String addrName = ExternalizedColumnInfo.toAddrColumnName(cleanName);

            String userComment = null;
            if (colDef.getComment() instanceof SQLCharExpr) {
                userComment = ((SQLCharExpr) colDef.getComment()).getText();
            }

            colDef.setName(SqlIdentifier.surroundWithBacktick(addrName));
            SQLDataTypeImpl varcharType = new SQLDataTypeImpl("varchar");
            varcharType.addArgument(new SQLIntegerExpr(ExternalizedColumnInfo.ADDR_VARCHAR_LENGTH));
            colDef.setDataType(varcharType);
            colDef.setExternalize(false);
            colDef.setCharsetExpr(null);
            colDef.setCollateExpr(null);
            colDef.getConstraints().clear();
            colDef.setDefaultExpr(null);
            colDef.setComment(new SQLCharExpr(
                ExternalizedColumnInfo.buildComment(originalType, userComment)));
        }

        return externalizedCols;
    }

    private void checkNoIndexOnExternalizedColumns(MySqlCreateTableStatement stmt, Set<String> externalizedCols) {
        // Check MySqlTableIndex (INDEX idx(col)) - skip columnar indexes (CCI)
        for (MySqlTableIndex index : stmt.getMysqlIndexes()) {
            if (index.isColumnar()) {
                continue;
            }
            for (SQLSelectOrderByItem item : index.getColumns()) {
                String colName = SQLUtils.normalizeNoTrim(item.getExpr().toString());
                if (externalizedCols.contains(colName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Cannot create index on externalized column '%s'.", colName));
                }
            }
        }
        // Check MySqlKey (KEY/UNIQUE KEY) - MySqlUnique extends MySqlKey
        for (MySqlKey key : stmt.getMysqlKeys()) {
            for (SQLSelectOrderByItem item : key.getColumns()) {
                String colName = SQLUtils.normalizeNoTrim(item.getExpr().toString());
                if (externalizedCols.contains(colName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Cannot create index on externalized column '%s'.", colName));
                }
            }
        }
    }

}
