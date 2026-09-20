package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnique;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiTableMetaBean;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionBy;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionDefinition;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionOptions;
import com.alibaba.polardbx.optimizer.parse.bean.TBPartitionBy;
import com.alibaba.polardbx.optimizer.parse.bean.TBPartitionDefinition;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sharding.DataNodeChooser;
import com.alibaba.polardbx.optimizer.utils.MetaUtils.TableColumns;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.newrule.IPartitionGen;
import com.alibaba.polardbx.optimizer.utils.newrule.ISubpartitionGen;
import com.alibaba.polardbx.optimizer.utils.newrule.RuleUtils;
import com.alibaba.polardbx.optimizer.utils.newrule.ShardFuncParamsChecker;
import com.alibaba.polardbx.optimizer.utils.newrule.TableRuleGenFactory;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.rule.ddl.PartitionByType;
import com.alibaba.polardbx.rule.impl.WrappedGroovyRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAddColumn;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterColumnDefaultVal;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlAlterTableRenameIndex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlConvertToCharacterSet;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlDropColumn;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.SqlTableOptions;
import org.apache.calcite.sql.SqlTruncateTable;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.polardbx.common.eagleeye.EagleeyeHelper.TEST_TABLE_PREFIX;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.convertTargetDB;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.fillGroup;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.filterGroup;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.getGroupIntersection;

/**
 * Data Definition Language LogicView
 *
 * @author hongxi.chx
 */
public class DataDefLanguageLogicView extends DDL {

    protected List<String> tableNames = new ArrayList<>();
    protected String schemaName;
    protected OptimizerContext optimizerContext;
    protected SqlNode sqlTemplate;
    protected PushDownOpt pushDownOpt;
    protected long lastInsertId = 0;
    protected long returnedLastInsertId = 0;
    protected RelOptTable table;
    protected List<RelNode> targetInput;

    protected TableRule tableRule;
    protected Map<String, List<List<String>>> targetTablesHintCache;

    /**
     * for GLOBAL SECONDARY INDEX, get table definition of main table
     */
    protected String dbIndex;
    protected List<String> phyTables;

    /**
     * for GLOBAL SECONDARY INDEX, target tables and rule for every index table
     */
    protected Map<String, Map<String, List<List<String>>>> gsiTargetTables;
    protected Map<String, Map<String, List<List<String>>>> gusiTargetTables;
    protected Map<String, Map<String, List<List<String>>>> gsiClusteredTargetTables;
    protected Map<String, Map<String, List<List<String>>>> gusiClusteredTargetTables;
    protected Map<String, TableRule> gsiTableRules;
    protected Map<String, SqlIndexDefinition> gsiIndexDefs;
    protected Map<String, SqlCreateIndex> gsiCreateIndexs;
    protected Map<String, List<RelNode>> gsiTargetInputs;
    protected Map<String, SqlNode> gsiParentSqlNode;
    protected Map<String, SqlNode> gsiNewIndexName;

    private String binName;
    private String truncateTableName;
    private SqlNode truncateCreate;

    // For alter table column with GSI.
    public enum AlterColumnSpecification {
        AlterColumnName, AlterColumnType, AlterColumnDefault, // Should start auto fill.
        AlterColumnComment, // May set to null and this flag is not set. Just push down this alter.
        AlterColumnOrder // Should alter order in metaDB first.
    }

    public final static Collection<AlterColumnSpecification> ALTER_COLUMN_RENAME =
        ImmutableList.of(AlterColumnSpecification.AlterColumnName);

    public final static Collection<AlterColumnSpecification> ALTER_COLUMN_NAME_OR_TYPE =
        ImmutableList.of(AlterColumnSpecification.AlterColumnName, AlterColumnSpecification.AlterColumnType);

    public final static Collection<AlterColumnSpecification> ALTER_COLUMN_DEFAULT =
        ImmutableList.of(AlterColumnSpecification.AlterColumnDefault);

    public final static Collection<AlterColumnSpecification> ALTER_COLUMN_REORDER =
        ImmutableList.of(AlterColumnSpecification.AlterColumnDefault);

    // Use list for multiple alters.
    private final List<Set<AlterColumnSpecification>> alterColumnSpecificationSets = new ArrayList<>();

    /**
     * Creates a <code>SingleRel</code>.
     */
    protected DataDefLanguageLogicView(RelOptCluster cluster, RelTraitSet traits, RelNode input, SqlNode sqlNode,
                                       SqlNode tableName) {
        super(cluster, traits.replace(DrdsConvention.INSTANCE), input);
        this.sqlNode = sqlNode;
        this.setTableName(tableName);
    }

    public DataDefLanguageLogicView(DDL logicView) {
        super(logicView.getCluster(), logicView.getTraitSet().replace(DrdsConvention.INSTANCE), logicView);
        this.setOperation(logicView.getOperation());
        this.rowType = logicView.getRowType();
        this.input = logicView.getInput();
        List<RelNode> relNodes = new ArrayList<>();
        relNodes.add(logicView);
        this.relNode = logicView.copy(logicView.getTraitSet(), relNodes);
        this.sqlNode = logicView.sqlNode;
        this.setNewTableName(logicView.getNewTableName());
        final SqlIdentifier tableName = (SqlIdentifier) logicView.getTableName();
        this.setTableName(tableName);
        this.setLikeTableName(logicView.getLikeTableName());
        if (tableName != null) {
            if (tableName.isSimple()) {
                this.tableNames = Arrays.asList(tableName.getSimple());
                this.schemaName = PlannerContext.getPlannerContext(logicView).getSchemaName();
            } else {
                final String schemaName = tableName.names.get(0);
                if (OptimizerContext.getContext(schemaName) == null) {
                    throw new TddlNestableRuntimeException("Unknown database " + schemaName);
                }
                this.tableNames = Arrays.asList(Util.last(tableName.names));
                this.schemaName = schemaName;
            }
        } else {
            this.schemaName = PlannerContext.getPlannerContext(logicView).getSchemaName();
        }

        SqlIdentifier newTableName = (SqlIdentifier) logicView.getNewTableName();
        if (newTableName != null && !newTableName.isSimple()) {
            String toSchemaName = newTableName.names.get(0);
            if (OptimizerContext.getContext(toSchemaName) == null) {
                throw new TddlNestableRuntimeException("Unknown target database " + toSchemaName);
            } else if (!StringUtils.equalsIgnoreCase(toSchemaName, this.schemaName)) {
                throw new TddlNestableRuntimeException("Target database must be the same as original database");
            }
        }

        this.optimizerContext = OptimizerContext.getContext(this.schemaName);

        gsiTableRules = new LinkedHashMap<>();
        gsiTargetTables = new LinkedHashMap<>();
        gusiTargetTables = new LinkedHashMap<>();
        gsiClusteredTargetTables = new LinkedHashMap<>();
        gusiClusteredTargetTables = new LinkedHashMap<>();
        gsiTargetInputs = new LinkedHashMap<>();
        gsiIndexDefs = new LinkedHashMap<>();
        gsiCreateIndexs = new LinkedHashMap<>();
        gsiParentSqlNode = new LinkedHashMap<>();
        gsiNewIndexName = new LinkedHashMap<>();
    }

    /**
     * 构建并获取其下层的 PhyDdlTableOperation 节点
     * <p>
     * <p>
     * <p>
     * <pre>
     *     计算分片
     *     构建对应的 PhyDdlTableOperation
     * </pre>
     */
    public List<RelNode> getInput(ExecutionContext executionContext, List<String> groupNames) {
        List relNodes = Arrays.asList();
        Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);

        for (Iterator<String> iterator = targetTables.keySet().iterator(); iterator.hasNext(); ) {
            String next = iterator.next();
            final List<List<String>> lists = targetTables.get(next);
            for (int i = 0; i < lists.size(); i++) {
                PhyDdlTableOperation phyDataDefLanguageOperation = new PhyDdlTableOperation(this);
                phyDataDefLanguageOperation.setDbIndex(next);
                phyDataDefLanguageOperation.setTableNames(lists);
                phyDataDefLanguageOperation.setExplain(false);
                phyDataDefLanguageOperation.setTableRule(tableRule);
                if (tableRule != null) {
                    phyDataDefLanguageOperation.setPartitioned(this.isPartition());
                }
                phyDataDefLanguageOperation.setShadowDbOnly(false);
                phyDataDefLanguageOperation.setDbType(DbType.MYSQL);
                phyDataDefLanguageOperation.setBytesSql(RelUtils.toNativeBytesSql(sqlNode));
                phyDataDefLanguageOperation.setSchemaName(this.schemaName);
                relNodes.add(phyDataDefLanguageOperation);
            }
        }
        return relNodes;
    }

    private boolean indexExistence(String tableName, String indexName) {
        final TableMeta tableMeta = optimizerContext.getLatestSchemaManager().getTable(tableName);
        if (null == tableMeta) {
            throw GeneralUtil.nestedException("Unknown table '" + tableName + "'.");
        }
        return tableMeta.getSecondaryIndexes().stream()
            .anyMatch(index -> index.getPhysicalIndexName().equalsIgnoreCase(indexName));
    }

    private String getGroupForLocality(String localityStr) {
        if (TStringUtil.isBlank(localityStr)) {
            return null;
        }
        LocalityDesc locality = LocalityDesc.parse(localityStr);
        if (locality.holdEmptyDnList()) {
            return null;
        }
        String storageInstId = locality.getDnList().get(0);
        List<GroupDetailInfoRecord> groups = DbTopologyManager.getGroupDetails(schemaName, storageInstId);
        Optional<GroupDetailInfoRecord> targetGroup = locality.chooseGroup(groups);
        if (!targetGroup.isPresent()) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("no storage instance match locality %s", locality));
        }
        return targetGroup.get().groupName;
    }

    public Map<String, List<List<String>>> getTargetTables(ExecutionContext executionContext) {
        Map<String, List<List<String>>> targetTables = new HashMap<>();
        if (null != getTargetTablesHintCache()) {
            targetTables.putAll(getTargetTablesHintCache());
        } else {
            targetTables.putAll(buildTargetTables(executionContext));
        }
        return targetTables;
    }

    /**
     * 考虑影子表、CREATE GSI 转 Create Table 等情况
     */
    protected Map<String, List<List<String>>> buildTargetTables(ExecutionContext executionContext) {

        Map<Integer, ParameterContext> param = executionContext.getParamMap();
        boolean forceFillGroup = false;
        final String schemaName = this.schemaName;
        if (sqlNode.getKind() == SqlKind.CREATE_SEQUENCE || sqlNode.getKind() == SqlKind.ALTER_SEQUENCE
            || sqlNode.getKind() == SqlKind.DROP_SEQUENCE || sqlNode.getKind() == SqlKind.RENAME_SEQUENCE) {
            TableRule tableRule = null;
            List<List<TargetDB>> targetDBs =
                DataNodeChooser.shardCreateTable(schemaName, getLogicalTableName(), this, tableRule);
            final Set<String> groupIntersection = getGroupIntersection(targetDBs);
            targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
            final List<Group> groups = optimizerContext.getMatrix().getGroups();
            targetDBs = fillGroup(targetDBs, groups, tableRule);
            return convertTargetDB(targetDBs);
        } else if (sqlNode.getKind() == SqlKind.CREATE_TABLE) {
            final SqlCreateTable sqlCreateTable = (SqlCreateTable) this.sqlNode;
            String logicalTableName = ((SqlIdentifier) sqlCreateTable.getName()).getLastName();
            final TableMeta tableToSchema = TableMetaParser.parse(logicalTableName, sqlCreateTable);

            tableToSchema.setSchemaName(schemaName);
            TddlRule tddlRule = null;
            if (sqlCreateTable.isShadow()) {
                tddlRule = optimizerContext.getRuleManager().getTddlRule();
                if (tddlRule != null) {
                    tddlRule.prepareForShadowTable(tableNames.get(0));
                }
            }

            boolean randomPhyTableNameEnabled = executionContext.isRandomPhyTableEnabled();

            TableRule tableRule = null;
            if (sqlCreateTable.isBroadCast()) {
                if (sqlCreateTable.getDbpartitionBy() != null || sqlCreateTable.getTbpartitionBy() != null) {
                    throw new IllegalArgumentException("broadcast and dbpartition are exclusive!");
                }
                tableRule = processBroadcast(tableNames.get(0), tableToSchema, this.optimizerContext,
                    randomPhyTableNameEnabled);
            } else if (sqlCreateTable.getDbpartitionBy() != null || sqlCreateTable.getTbpartitionBy() != null) {
                boolean supportSingleDbMultiTbs = checkIfSupportSingleDbMultiTbs(sqlCreateTable);
                if (ConfigDataMode.isPolarDbX() && !supportSingleDbMultiTbs) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "A single database shard with multiple table shards is not allowed in PolarDB-X");
                }

                // tableRule = process(((SqlIdentifier)
                // sqlCreateTable.getOperandList().get(0)).getSimple(),tableToSchema);
                tableRule = buildTableRule(tableNames.get(0), tableToSchema, sqlCreateTable.getDbpartitionBy(),
                    sqlCreateTable.getDbpartitions(), sqlCreateTable.getTbpartitionBy(),
                    sqlCreateTable.getTbpartitions(), sqlCreateTable.getMappingRules(), this.optimizerContext,
                    executionContext);

            } else {
                // DRDS模式需要给发0库单表发规则
                tableRule = new TableRule();
                // 伪造一个topo,用于发规则

                String defaultDbIndex = optimizerContext.getRuleManager().getDefaultDbIndex(null);
                String group = getGroupForLocality(sqlCreateTable.getLocality());
                if (group != null) {
                    defaultDbIndex = group;
                }

                tableRule.setRandomTableNamePatternEnabled(randomPhyTableNameEnabled);

                String tableName = Util.last(((SqlIdentifier) sqlCreateTable.getOperandList().get(0)).names);

                populateExistingRandomSuffix(tableName, tableRule, optimizerContext, randomPhyTableNameEnabled);

                if (randomPhyTableNameEnabled) {
                    tableName = RuleUtils.genTableNameWithRandomSuffix(tableRule, tableName);
                }

                Map<String, Set<String>> topo = new HashMap<>();
                topo.put(defaultDbIndex, new HashSet<>(Arrays.asList(tableName)));
                tableRule.setActualTopology(topo);
                tableRule.setBroadcast(false);
                tableRule.setDbNamePattern(defaultDbIndex);
                tableRule.setTbNamePattern(tableName);
            }
            this.tableRule = tableRule;
            if (ConfigDataMode.isFastMock()) {
                tableRule.init();
            }
            List<List<TargetDB>> targetDBs =
                DataNodeChooser.shardCreateTable(schemaName, getLogicalTableName(), this, tableRule);
            final Set<String> groupIntersection = getGroupIntersection(targetDBs);
            targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
            final List<Group> groups = optimizerContext.getMatrix().getGroups();
            targetDBs = fillGroup(targetDBs, groups, tableRule);
            if (tableRule != null && !PlannerUtils.isSingleTable(tableRule) && !tableRule.isBroadcast()) {
                this.setPartition(true);
            }

            if (sqlCreateTable.createGsi()) {
                // build table rule and target tables for every index table
                if (null != sqlCreateTable.getGlobalKeys()) {
                    for (Pair<SqlIdentifier, SqlIndexDefinition> gsi : sqlCreateTable.getGlobalKeys()) {

                        final String indexTableName = RelUtils.lastStringValue(gsi.getKey());
                        final SqlIndexDefinition indexDef = gsi.getValue();

                        if (sqlCreateTable.isShadow() && tddlRule != null) {
                            tddlRule.prepareForShadowTable(indexTableName);
                        }

                        buildGsiTargetTable(tableToSchema, indexTableName, indexDef, gsiTargetTables, executionContext);

                        if (sqlCreateTable.isShadow() && tddlRule != null) {
                            tddlRule.cleanupForShadowTable(indexTableName);
                        }
                    }
                }

                if (null != sqlCreateTable.getGlobalUniqueKeys()) {
                    for (Pair<SqlIdentifier, SqlIndexDefinition> gsi : sqlCreateTable.getGlobalUniqueKeys()) {

                        final String indexTableName = RelUtils.lastStringValue(gsi.getKey());
                        final SqlIndexDefinition indexDef = gsi.getValue();

                        if (sqlCreateTable.isShadow() && tddlRule != null) {
                            tddlRule.prepareForShadowTable(indexTableName);
                        }

                        buildGsiTargetTable(tableToSchema, indexTableName, indexDef, gusiTargetTables,
                            executionContext);

                        if (sqlCreateTable.isShadow() && tddlRule != null) {
                            tddlRule.cleanupForShadowTable(indexTableName);
                        }
                    }
                }

                if (null != sqlCreateTable.getClusteredKeys()) {
                    for (Pair<SqlIdentifier, SqlIndexDefinition> gsi : sqlCreateTable.getClusteredKeys()) {

                        final String indexTableName = RelUtils.lastStringValue(gsi.getKey());
                        final SqlIndexDefinition indexDef = gsi.getValue();

                        if (sqlCreateTable.isShadow() && tddlRule != null) {
                            tddlRule.prepareForShadowTable(indexTableName);
                        }

                        buildGsiTargetTable(tableToSchema, indexTableName, indexDef, gsiClusteredTargetTables,
                            executionContext);

                        if (sqlCreateTable.isShadow() && tddlRule != null) {
                            tddlRule.cleanupForShadowTable(indexTableName);
                        }
                    }
                }

                if (null != sqlCreateTable.getClusteredUniqueKeys()) {
                    for (Pair<SqlIdentifier, SqlIndexDefinition> gsi : sqlCreateTable.getClusteredUniqueKeys()) {

                        final String indexTableName = RelUtils.lastStringValue(gsi.getKey());
                        final SqlIndexDefinition indexDef = gsi.getValue();

                        if (sqlCreateTable.isShadow() && tddlRule != null) {
                            tddlRule.prepareForShadowTable(indexTableName);
                        }

                        buildGsiTargetTable(tableToSchema, indexTableName, indexDef, gusiClusteredTargetTables,
                            executionContext);

                        if (sqlCreateTable.isShadow() && tddlRule != null) {
                            tddlRule.cleanupForShadowTable(indexTableName);
                        }
                    }
                }
            }

            if (tddlRule != null) {
                tddlRule.cleanupForShadowTable(tableNames.get(0));
            }

            return convertTargetDB(targetDBs);
        } else if (sqlNode.getKind() == SqlKind.CREATE_VIEW) {
            List<List<TargetDB>> targetDBs = DataNodeChooser.shard(this, true, executionContext);
            final Set<String> groupIntersection = getGroupIntersection(targetDBs);
            targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
            final List<Group> groups = optimizerContext.getMatrix().getGroups();
            targetDBs = fillGroup(targetDBs, groups, null);
            return convertTargetDB(targetDBs);
        } else if (sqlNode.getKind() == SqlKind.RENAME_TABLE) {
            this.tableRule = optimizerContext.getRuleManager().getTddlRule().getTable(getLogicalTableName());
            if (tableRule != null && !PlannerUtils.isSingleTable(tableRule) && !tableRule.isBroadcast()) {
                this.setPartition(true);
            }
            List<List<TargetDB>> targetDBs = DataNodeChooser.shard(this, true, executionContext);
            final Set<String> groupIntersection = getGroupIntersection(targetDBs);
            targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
            final List<Group> groups = optimizerContext.getMatrix().getGroups();
            targetDBs = fillGroup(targetDBs, groups, tableRule);
            final Map<String, List<List<String>>> stringListMap = convertTargetDB(targetDBs);
            fillRenamePhyTable(schemaName, stringListMap, ((SqlRenameTable) sqlNode).getRenamedName(),
                getLogicalTableName());
            return stringListMap;
        } else if (sqlNode.getKind() == SqlKind.CREATE_INDEX && ((SqlCreateIndex) sqlNode).createGsi()) {
            final SqlCreateIndex sqlCreateIndex = (SqlCreateIndex) sqlNode;

            final TableMeta mainTableSchema =
                executionContext.getSchemaManager(schemaName).getTable(getLogicalTableName());
            final String indexTableName = RelUtils.lastStringValue(sqlCreateIndex.getIndexName());

            TddlRule tddlRule = optimizerContext.getRuleManager().getTddlRule();
            if (tddlRule != null && TStringUtil.startsWithIgnoreCase(getLogicalTableName(), TEST_TABLE_PREFIX)) {
                tddlRule.prepareForShadowTable(indexTableName);
            }

            tableRule =
                buildGsiTargetTable(mainTableSchema, indexTableName, sqlCreateIndex, gsiTargetTables, executionContext);

            if (tddlRule != null && TStringUtil.startsWithIgnoreCase(getLogicalTableName(), TEST_TABLE_PREFIX)) {
                tddlRule.cleanupForShadowTable(indexTableName);
            }

            // Create normal local index on clustered.
            final String tableName = getLogicalTableName();
            final GsiMetaBean gsiMetaBean =
                optimizerContext.getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);

            if (gsiMetaBean.withGsi(tableName)) {
                // Drop local index. Also do on clustered.
                final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
                for (Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                    if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getValue().columnarIndex) {
                        final String clusteredTableName = gsiEntry.getKey();
                        final Map<String, List<List<String>>> targetTables =
                            buildTargetTables(param, schemaName, clusteredTableName, executionContext);
                        this.gsiClusteredTargetTables.put(clusteredTableName, targetTables);
                    }
                }
            }

            // Get primary table input.
            final TableRule primaryTableRule =
                optimizerContext.getRuleManager().getTddlRule().getTable(getLogicalTableName());
            List<List<TargetDB>> targetDBs = DataNodeChooser.shard(this, true, executionContext);
            final Set<String> groupIntersection = getGroupIntersection(targetDBs);
            targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
            final List<Group> groups = optimizerContext.getMatrix().getGroups();

            targetDBs = fillGroup(targetDBs, groups, primaryTableRule);
            return convertTargetDB(targetDBs, schemaName);
        } else if (sqlNode.getKind() == SqlKind.ALTER_TABLE && ((SqlAlterTable) sqlNode).createGsi()) {
            final SqlAlterTable sqlAlterTable = (SqlAlterTable) sqlNode;
            final SqlIndexDefinition indexDef = ((SqlAddIndex) sqlAlterTable.getAlters().get(0)).getIndexDef();

            final TableMeta mainTableSchema =
                executionContext.getSchemaManager(schemaName).getTable(getLogicalTableName());
            final String indexTableName = RelUtils.lastStringValue(indexDef.getIndexName());

            TddlRule tddlRule = optimizerContext.getRuleManager().getTddlRule();
            if (tddlRule != null && TStringUtil.startsWithIgnoreCase(getLogicalTableName(), TEST_TABLE_PREFIX)) {
                tddlRule.prepareForShadowTable(indexTableName);
            }

            if (sqlNode instanceof SqlAlterTablePartitionKey) {
                tableRule =
                    buildGsiTargetTableForRepartition(mainTableSchema, indexTableName, indexDef, gsiTargetTables,
                        executionContext);
            } else {
                tableRule =
                    buildGsiTargetTable(mainTableSchema, indexTableName, indexDef, gsiTargetTables, executionContext);
            }

            if (tddlRule != null && TStringUtil.startsWithIgnoreCase(getLogicalTableName(), TEST_TABLE_PREFIX)) {
                tddlRule.cleanupForShadowTable(indexTableName);
            }

            if (sqlNode instanceof SqlAlterTablePartitionKey) {
                // Alter partition, keep original input.
                return gsiTargetTables.get(indexTableName);
            }

            // Create normal local index on clustered.
            final String tableName = getLogicalTableName();
            final GsiMetaBean gsiMetaBean =
                optimizerContext.getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);

            if (gsiMetaBean.withGsi(tableName)) {
                // Drop local index. Also do on clustered.
                final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
                for (Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                    if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getValue().columnarIndex) {
                        final String clusteredTableName = gsiEntry.getKey();
                        final Map<String, List<List<String>>> targetTables =
                            buildTargetTables(param, schemaName, clusteredTableName, executionContext);
                        this.gsiClusteredTargetTables.put(clusteredTableName, targetTables);
                    }
                }
            }

            // Get primary table input.
            final TableRule primaryTableRule =
                optimizerContext.getRuleManager().getTddlRule().getTable(getLogicalTableName());
            List<List<TargetDB>> targetDBs = DataNodeChooser.shard(this, true, executionContext);
            final Set<String> groupIntersection = getGroupIntersection(targetDBs);
            targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
            final List<Group> groups = optimizerContext.getMatrix().getGroups();

            targetDBs = fillGroup(targetDBs, groups, primaryTableRule);
            return convertTargetDB(targetDBs, schemaName);
        } else {
            if (sqlNode.getKind() == SqlKind.ALTER_TABLE) {
                final SqlAlterTable alterTable = (SqlAlterTable) sqlNode;

                if (alterTable.dropIndex()) {
                    final SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) alterTable.getAlters().get(0);
                    final String tableName = alterTable.getOriginTableName().getLastName();
                    final String indexTableName = dropIndex.getIndexName().getLastName();

                    final GsiMetaBean gsiMetaBean =
                        executionContext.getSchemaManager(schemaName).getGsi(tableName, IndexStatus.ALL);

                    if (gsiMetaBean.isGsi(indexTableName)) {
                        final Map<String, List<List<String>>> targetTables =
                            buildGsiTargetTables(param, schemaName, indexTableName, executionContext);

                        this.gsiTargetTables.put(indexTableName, targetTables);

                        // Add clustered index.
                        if (gsiMetaBean.withGsi(tableName)) {
                            // Drop generated local index on clustered.
                            final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
                            for (Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                                if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getKey()
                                    .equalsIgnoreCase(indexTableName) && !gsiEntry.getValue().columnarIndex) {
                                    // Add all clustered index except which is dropping.
                                    final String clusteredTableName = gsiEntry.getKey();
                                    final Map<String, List<List<String>>> targetTablesClustered =
                                        buildTargetTables(param, schemaName, clusteredTableName, executionContext);
                                    this.gsiClusteredTargetTables.put(clusteredTableName, targetTablesClustered);
                                }
                            }
                        }
                    } else {
                        // Dealing drop local index on clustered index.
                        buildAlterGsiTargetTables(param, schemaName, alterTable, executionContext);
                    }
                } else if (alterTable.renameIndex()) {
                    final SqlAlterTableRenameIndex renameIndex =
                        (SqlAlterTableRenameIndex) alterTable.getAlters().get(0);

                    final String tableName = getLogicalTableName();
                    final String indexTableName = renameIndex.getIndexName().getLastName();
                    final String newIndexName = renameIndex.getNewIndexNameStr();

                    final GsiMetaBean gsiMetaBean =
                        executionContext.getSchemaManager(schemaName).getGsi(tableName, IndexStatus.ALL);

                    if (gsiMetaBean.isGsi(indexTableName)) {
                        TableRule tableRule = optimizerContext.getRuleManager().getTddlRule().getTable(indexTableName);
                        if (tableRule != null && !PlannerUtils.isSingleTable(tableRule) && !tableRule.isBroadcast()) {
                            this.setPartition(true);
                        }

                        final Map<String, List<List<String>>> targetTables =
                            buildGsiTargetTables(param, schemaName, indexTableName, executionContext);

                        fillRenamePhyTable(schemaName, targetTables, newIndexName, indexTableName);

                        this.gsiTargetTables.put(indexTableName, targetTables);
                    }
                } else {
                    buildAlterGsiTargetTables(param, schemaName, alterTable, executionContext);
                }
            } else if (sqlNode.getKind() == SqlKind.CREATE_INDEX) {
                // Create normal local index.
                final SqlCreateIndex createIndex = (SqlCreateIndex) sqlNode;
                final String tableName = createIndex.getOriginTableName().getLastName();

                final GsiMetaBean gsiMetaBean =
                    optimizerContext.getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);

                if (gsiMetaBean.withGsi(tableName)) {
                    // Drop local index. Also do on clustered.
                    final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
                    for (Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                        if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getValue().columnarIndex) {
                            final String clusteredTableName = gsiEntry.getKey();
                            final Map<String, List<List<String>>> targetTables =
                                buildTargetTables(param, schemaName, clusteredTableName, executionContext);
                            this.gsiClusteredTargetTables.put(clusteredTableName, targetTables);
                        }
                    }
                }
            } else if (sqlNode.getKind() == SqlKind.DROP_INDEX) {
                final SqlDropIndex dropIndex = (SqlDropIndex) sqlNode;
                final String tableName = dropIndex.getOriginTableName().getLastName();
                final String indexTableName = dropIndex.getIndexName().getLastName();

                final GsiMetaBean gsiMetaBean =
                    executionContext.getSchemaManager(schemaName).getGsi(tableName, IndexStatus.ALL);

                if (gsiMetaBean.isGsi(indexTableName)) {
                    final Map<String, List<List<String>>> targetTables =
                        buildGsiTargetTables(param, schemaName, indexTableName, executionContext);

                    this.gsiTargetTables.put(indexTableName, targetTables);

                    // Add clustered index.
                    if (gsiMetaBean.withGsi(tableName)) {
                        // Drop generated local index on clustered.
                        final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
                        for (Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                            if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getKey()
                                .equalsIgnoreCase(indexTableName) && !gsiEntry.getValue().columnarIndex) {
                                // Add all clustered index except which is dropping.
                                final String clusteredTableName = gsiEntry.getKey();
                                final Map<String, List<List<String>>> targetTablesClustered =
                                    buildTargetTables(param, schemaName, clusteredTableName, executionContext);
                                this.gsiClusteredTargetTables.put(clusteredTableName, targetTablesClustered);
                            }
                        }
                    }
                } else if (gsiMetaBean.withGsi(tableName)) {
                    // Drop local index. Also do on clustered.
                    final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
                    for (Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                        if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getValue().columnarIndex) {
                            final String clusteredTableName = gsiEntry.getKey();
                            if (indexExistence(clusteredTableName, indexTableName)) {
                                // Add to job only if existence.
                                final Map<String, List<List<String>>> targetTables =
                                    buildTargetTables(param, schemaName, clusteredTableName, executionContext);
                                this.gsiClusteredTargetTables.put(clusteredTableName, targetTables);
                            }
                        }
                    }
                }
            } else if (sqlNode.getKind() == SqlKind.DROP_TABLE) {
                final SqlDropTable dropTable = (SqlDropTable) sqlNode;
                final String tableName = dropTable.getOriginTableName().getLastName();

                final GsiMetaBean gsiMetaBean =
                    executionContext.getSchemaManager(schemaName).getGsi(tableName, IndexStatus.ALL);

                if (gsiMetaBean.withGsi(tableName)) {
                    // handle gsi table
                    final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
                    for (Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                        if (!gsiEntry.getValue().columnarIndex) {
                            final String indexTableName = gsiEntry.getKey();
                            final Map<String, List<List<String>>> targetTables =
                                buildGsiTargetTables(param, schemaName, indexTableName, executionContext);

                            this.gsiTargetTables.put(indexTableName, targetTables);
                        }
                    }
                }
            } else if (sqlNode.getKind() == SqlKind.TRUNCATE_TABLE) {
                final SqlTruncateTable truncateTable = (SqlTruncateTable) sqlNode;
                final String tableName = RelUtils.lastStringValue(truncateTable.getName());

                final GsiMetaBean gsiMetaBean =
                    executionContext.getSchemaManager(schemaName).getGsi(tableName, IndexStatus.ALL);

                if (gsiMetaBean.withGsi(tableName)) {
                    // handle gsi table
                    final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
                    for (Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                        if (!gsiEntry.getValue().columnarIndex) {
                            final String indexTableName = gsiEntry.getKey();
                            final Map<String, List<List<String>>> targetTables =
                                buildGsiTargetTables(param, schemaName, indexTableName, executionContext);

                            this.gsiTargetTables.put(indexTableName, targetTables);
                        }
                    }
                }
            } else if (sqlNode.getKind() == SqlKind.CREATE_TRIGGER) {

            } else if (sqlNode.getKind() == SqlKind.DROP_TRIGGER) {

            }

            this.tableRule = optimizerContext.getRuleManager().getTddlRule().getTable(getLogicalTableName());
            if (tableRule != null && !PlannerUtils.isSingleTable(tableRule) && !tableRule.isBroadcast()) {
                this.setPartition(true);
            }

            PlannerContext context = (PlannerContext) this.getCluster().getPlanner().getContext();
            boolean enableAllowAlterShardKey =
                context.getParamManager().getBoolean(ConnectionParams.ENABLE_ALTER_SHARD_KEY);

            if (sqlNode instanceof SqlAlterTable & !enableAllowAlterShardKey) {
                final Map<SqlAlterTable.ColumnOpt, List<String>> columnOpts = ((SqlAlterTable) sqlNode).getColumnOpts();
                final Set<Map.Entry<SqlAlterTable.ColumnOpt, List<String>>> entries = columnOpts.entrySet();
                for (Map.Entry<SqlAlterTable.ColumnOpt, List<String>> entry : entries) {
                    final List<String> value = entry.getValue();
                    for (int i = 0; i < value.size(); i++) {
                        final String s = value.get(i);
                        if (containsShardingColumn(s)) {
                            throw new IllegalArgumentException("Can't " + entry.getKey().name() + " shard column:" + s);
                        }
                    }

                }
            }

            List<List<TargetDB>> targetDBs = DataNodeChooser.shard(this, true, executionContext);
            final Set<String> groupIntersection = getGroupIntersection(targetDBs);
            targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
            final List<Group> groups = optimizerContext.getMatrix().getGroups();

            targetDBs = fillGroup(targetDBs, groups, tableRule, forceFillGroup);
            return convertTargetDB(targetDBs, schemaName);
        }
    }

    private boolean checkIfSupportSingleDbMultiTbs(SqlCreateTable sqlCreateTable) {
        int dbCount = 1, tbCount = 1;

        if (sqlCreateTable.getDbpartitions() != null) {
            dbCount = ((SqlLiteral) sqlCreateTable.getDbpartitions()).intValue(false);
        }

        if (sqlCreateTable.getTbpartitions() != null) {
            tbCount = ((SqlLiteral) sqlCreateTable.getTbpartitions()).intValue(false);
        }

        boolean singleDb =
            sqlCreateTable.getDbpartitionBy() == null || (sqlCreateTable.getDbpartitions() != null && dbCount == 1);
        boolean multiTbs = sqlCreateTable.getTbpartitionBy() != null && tbCount > 1;

        return DynamicConfig.getInstance().isSupportSingleDbMultiTbs() || !(singleDb && multiTbs);
    }

    private void updateAlterColumnSpecification(ColumnMeta columnMeta, SqlColumnDeclaration columnDeclaration,
                                                Set<AlterColumnSpecification> specificationSet) {
        // Check basic type.
        final RelDataType targetDataType = columnDeclaration.getDataType().deriveType(getCluster().getTypeFactory());
        if (!SqlTypeUtil.equalSansNullability(getCluster().getTypeFactory(), targetDataType,
            columnMeta.getField().getRelType())) {
            specificationSet.add(AlterColumnSpecification.AlterColumnType);
        }

        // Check nullable.
        final boolean targetNullable = null == columnDeclaration.getNotNull()
            || SqlColumnDeclaration.ColumnNull.NULL == columnDeclaration.getNotNull();
        if (columnMeta.getField().getRelType().isNullable() != targetNullable) {
            specificationSet.add(AlterColumnSpecification.AlterColumnType);
        }

        // Check default value.
        final String originalDefault = null == columnMeta.getField().getDefault() ?
            (columnMeta.getField().getRelType().isNullable() ? "NULL" : null) : columnMeta.getField().getDefault();
        final String targetDefault;
        if (columnDeclaration.getDefaultExpr() != null) {
            targetDefault = columnDeclaration.getDefaultExpr().getOperator().getName();
        } else if (columnDeclaration.getDefaultVal() != null) {
            targetDefault = columnDeclaration.getDefaultVal().toValue();
        } else if (targetNullable) {
            targetDefault = "NULL"; // Default null.
        } else {
            targetDefault = null;
        }
        if ((null == originalDefault && targetDefault != null) || (originalDefault != null && null == targetDefault)
            || (originalDefault != null && targetDefault != null && !originalDefault.equals(targetDefault))) {
            specificationSet.add(AlterColumnSpecification.AlterColumnDefault);
        }

        // Check comment.
        if (columnDeclaration.getComment() != null) {
            specificationSet.add(AlterColumnSpecification.AlterColumnComment);
        }
    }

    private Set<AlterColumnSpecification> getAlterColumnSpecification(TableMeta tableMeta,
                                                                      SqlAlterSpecification specification) {
        final Set<AlterColumnSpecification> specificationSet = new HashSet<>();

        switch (specification.getKind()) {
        case CHANGE_COLUMN: {
            final SqlChangeColumn changeColumn = (SqlChangeColumn) specification;

            // Check name.
            final String oldName = changeColumn.getOldName().getLastName();
            if (!changeColumn.getNewName().getLastName().equalsIgnoreCase(oldName)) {
                specificationSet.add(AlterColumnSpecification.AlterColumnName);
            }

            // Check definition.
            final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(oldName);
            if (null == columnMeta) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Modify unknown column '" + oldName + "'");
            }
            updateAlterColumnSpecification(columnMeta, changeColumn.getColDef(), specificationSet);

            // Check reorder.
            if (changeColumn.isFirst()) {
                // Check whether first column.
                if (!tableMeta.getPhysicalColumns().get(0).getName().equalsIgnoreCase(oldName)) {
                    specificationSet.add(AlterColumnSpecification.AlterColumnOrder);
                }
            } else if (changeColumn.getAfterColumn() != null) {
                final String afterColName = changeColumn.getAfterColumn().getLastName();
                for (int colIdx = 0; colIdx < tableMeta.getPhysicalColumns().size(); ++colIdx) {
                    final ColumnMeta probCol = tableMeta.getPhysicalColumns().get(colIdx);
                    if (probCol.getName().equalsIgnoreCase(afterColName)) {
                        // Find the before col.
                        if (colIdx >= tableMeta.getPhysicalColumns().size() - 1 || !tableMeta.getPhysicalColumns()
                            .get(colIdx + 1).getName().equalsIgnoreCase(oldName)) {
                            specificationSet.add(AlterColumnSpecification.AlterColumnOrder);
                        }
                        break;
                    }
                }
            } // Or change in place.
        }
        break;

        case MODIFY_COLUMN: {
            final SqlModifyColumn modifyColumn = (SqlModifyColumn) specification;

            // Modify doesn't change the name.
            // Now check definition.
            final String colName = modifyColumn.getColName().getLastName();
            final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(colName);
            if (null == columnMeta) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Modify unknown column '" + colName + "'");
            }
            updateAlterColumnSpecification(columnMeta, modifyColumn.getColDef(), specificationSet);

            // Check reorder.
            if (modifyColumn.isFirst()) {
                // Check whether first column.
                if (!tableMeta.getPhysicalColumns().get(0).getName().equalsIgnoreCase(colName)) {
                    specificationSet.add(AlterColumnSpecification.AlterColumnOrder);
                }
            } else if (modifyColumn.getAfterColumn() != null) {
                final String afterColName = modifyColumn.getAfterColumn().getLastName();
                for (int colIdx = 0; colIdx < tableMeta.getPhysicalColumns().size(); ++colIdx) {
                    final ColumnMeta probCol = tableMeta.getPhysicalColumns().get(colIdx);
                    if (probCol.getName().equalsIgnoreCase(afterColName)) {
                        // Find the before col.
                        if (colIdx >= tableMeta.getPhysicalColumns().size() - 1 || !tableMeta.getPhysicalColumns()
                            .get(colIdx + 1).getName().equalsIgnoreCase(colName)) {
                            specificationSet.add(AlterColumnSpecification.AlterColumnOrder);
                        }
                        break;
                    }
                }
            } // Or modify in place.
        }
        break;

        default:
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Unknown alter specification");
        }
        return specificationSet;
    }

    protected void buildAlterGsiTargetTables(Map<Integer, ParameterContext> param, String schemaName,
                                             SqlAlterTable alterTable, ExecutionContext ec) {
        final String tableName = getLogicalTableName();

        final TableMeta table = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);

        if (table.withGsi()) {
            final TableColumns tableColumns = TableColumns.build(table);
            String columnName = null;
            SqlKind alterType = null;

            // Clear column specifications.
            alterColumnSpecificationSets.clear();

            boolean gsiExists = false;
            boolean clusteredExists = false;
            for (SqlAlterSpecification alterItem : alterTable.getAlters()) {
                if (!(alterItem.isA(SqlKind.CHECK_ALTER_WITH_GSI))) {
                    continue;
                }

                final PlannerContext context = (PlannerContext) this.getCluster().getPlanner().getContext();

                alterType = alterItem.getKind();
                switch (alterType) {
                case ADD_COLUMN:
                    SqlAddColumn addColumn = (SqlAddColumn) alterItem;
                    if (table.withClustered()) {
                        if (alterTable.getColumnOpts().size() > 1 || !alterTable.getColumnOpts()
                            .containsKey(SqlAlterTable.ColumnOpt.ADD)
                            || alterTable.getColumnOpts().get(SqlAlterTable.ColumnOpt.ADD).size() > 1
                            || alterTable.getAlters().size() > 1) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support mix ADD COLUMN with other ALTER statements when table contains CLUSTERED INDEX");
                        }
                        // Check duplicated column name for clustered index, because this may generate a compound job.
                        final String colName = addColumn.getColName().getLastName();
                        if (table.getColumnIgnoreCase(colName) != null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                                "Duplicate column name '" + colName + "' on `" + tableName + "`");
                        }
                        // Check in GSI table. This should never happen.
                        if (tableColumns.existsInGsi(colName)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                                "Duplicate column name '" + colName + "' on GSI of `" + tableName + "`");
                        }
                        clusteredExists = true;
                    }
                    break;
                case ALTER_COLUMN_DEFAULT_VAL:
                    SqlAlterColumnDefaultVal alterDefaultVal = (SqlAlterColumnDefaultVal) alterItem;
                    columnName = alterDefaultVal.getColumnName().getLastName();
                    if (tableColumns.existsInGsi(columnName)) {
                        gsiExists = true;
                        if (!ConfigDataMode.isPolarDbX() && !context.getParamManager()
                            .getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support drop or set default value of column included in global secondary index");
                        }
                    }
                    break;
                case CHANGE_COLUMN:
                    SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                    columnName = changeColumn.getOldName().getLastName();
                    if (tableColumns.existsInGsi(columnName)) {
                        gsiExists = true;

                        if (ConfigDataMode.isPolarDbX()) {
                            // Allow some special case of modify column.
                            final Set<AlterColumnSpecification> specificationSet =
                                getAlterColumnSpecification(table, changeColumn);
                            alterColumnSpecificationSets.add(specificationSet);

                            if (specificationSet.stream().anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)) {
                                if ((tableColumns.isPrimaryKey(columnName) || tableColumns.isShardingKey(columnName)
                                    || tableColumns.isGsiShardingKey(columnName))) {
                                    if (!context.getParamManager()
                                        .getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                            "Do not support change column name or type on primary key or sharding key on table with GSI");
                                    }
                                } else if (tableColumns.existsInGsiUniqueKey(columnName, false)) {
                                    if (!context.getParamManager()
                                        .getBoolean(ConnectionParams.ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI)
                                        && !context.getParamManager()
                                        .getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                            "Change column included in UGSI is extremely dangerous which may corrupt the unique constraint");
                                    }
                                } else if (!context.getParamManager()
                                    .getBoolean(ConnectionParams.ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI)
                                    && !context.getParamManager()
                                    .getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                        "Change column name or type included in GSI is not recommended");
                                }
                            } // Alter default, comment and order, so just let it go.

                            // Change alter warning.
                            if (!context.getParamManager()
                                .getBoolean(ConnectionParams.ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI)
                                && !context.getParamManager().getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                if (1 == specificationSet.size() && specificationSet.stream()
                                    .anyMatch(ALTER_COLUMN_DEFAULT::contains)) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                        "It seems that you only alter column default, try ALTER COLUMN SET/DROP DEFAULT(partly rollback supported) instead for better practice");
                                }
                            }
                        } else if (!context.getParamManager().getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support change column included in global secondary index");
                        }
                    }
                    break;
                case DROP_COLUMN:
                    SqlDropColumn dropColumn = (SqlDropColumn) alterItem;
                    columnName = dropColumn.getColName().getLastName();

                    if (tableColumns.existsInGsi(columnName)) {
                        gsiExists = true;

                        // PK can never modified.
                        if (tableColumns.primaryKeys.contains(columnName)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support drop column included in primary key of table which has global secondary index");
                        }

                        // Drop column in local unique key and also in GSI is allowed in PolarDB-X by hint.
                        // Note this is **DANGER** because this operation may partly success and can't recover or rollback.
                        if ((!ConfigDataMode.isPolarDbX() || !context.getParamManager()
                            .getBoolean(ConnectionParams.ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI))
                            && tableColumns.existsInLocalUniqueKey(columnName, false)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support drop column included in unique key of table which has global secondary index");
                        }

                        // Drop column in GSI is now allowed in PolarDB-X.
                        if (!ConfigDataMode.isPolarDbX() && !context.getParamManager()
                            .getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support drop column included in global secondary index");
                        }
                    }

                    // Sharding key can never modified.
                    if (tableColumns.isGsiShardingKey(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support drop sharding key of global secondary index");
                    }

                    // Drop column in GSI unique key is allowed in PolarDB-X by hint.
                    // Note this is **DANGER** because this operation may partly success and can't recover or rollback.
                    if ((!ConfigDataMode.isPolarDbX() || !context.getParamManager()
                        .getBoolean(ConnectionParams.ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI))
                        && tableColumns.existsInGsiUniqueKey(columnName, false)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support drop column included in unique key of global secondary index");
                    }
                    break;
                case MODIFY_COLUMN:
                    SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                    columnName = modifyColumn.getColName().getLastName();

                    if (tableColumns.existsInGsi(columnName)) {
                        gsiExists = true;

                        if (ConfigDataMode.isPolarDbX()) {
                            // Allow some special case of modify column.
                            final Set<AlterColumnSpecification> specificationSet =
                                getAlterColumnSpecification(table, modifyColumn);
                            alterColumnSpecificationSets.add(specificationSet);

                            if (specificationSet.stream().anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)) {
                                if ((tableColumns.isPrimaryKey(columnName) || tableColumns.isShardingKey(columnName)
                                    || tableColumns.isGsiShardingKey(columnName))) {
                                    if (!context.getParamManager()
                                        .getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                            "Do not support change column name or type on primary key or sharding key on table with GSI");
                                    }
                                } else if (tableColumns.existsInGsiUniqueKey(columnName, false)) {
                                    if (!context.getParamManager()
                                        .getBoolean(ConnectionParams.ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI)
                                        && !context.getParamManager()
                                        .getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                            "Change column included in UGSI is extremely dangerous which may corrupt the unique constraint");
                                    }
                                } else if (!context.getParamManager()
                                    .getBoolean(ConnectionParams.ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI)
                                    && !context.getParamManager()
                                    .getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                        "Change column name or type included in GSI is not recommended");
                                }
                            } // Alter default, comment and order, so just let it go.

                            // Modify alter warning.
                            if (!context.getParamManager()
                                .getBoolean(ConnectionParams.ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI)
                                && !context.getParamManager().getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                if (1 == specificationSet.size() && specificationSet.stream()
                                    .anyMatch(ALTER_COLUMN_DEFAULT::contains)) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                        "It seems that you only alter column default, try ALTER COLUMN SET/DROP DEFAULT(partly rollback supported) instead for better practice");
                                }
                            }
                        } else if (!context.getParamManager().getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support modify column included in global secondary index");
                        }
                    }
                    break;
                case ADD_INDEX:
                case ADD_UNIQUE_INDEX:
                case ADD_FULL_TEXT_INDEX:
                case ADD_SPATIAL_INDEX:
                case ADD_FOREIGN_KEY:
                    final SqlAddIndex addIndex = (SqlAddIndex) alterItem;
                    if (null != addIndex.getIndexName() && table.withGsi(addIndex.getIndexName().getLastName())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Duplicated index name " + addIndex.getIndexName().getLastName());
                    }
                    // Fall over.
                case DROP_INDEX:
                    clusteredExists = true;
                    break;
                case DROP_PRIMARY_KEY:
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Does not support drop primary key from table with global secondary index");
                case CONVERT_TO_CHARACTER_SET:
                    gsiExists = true;
                    if (!context.getParamManager().getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                        // Check correctness. Because this can not rollback.
                        final SqlConvertToCharacterSet convert = (SqlConvertToCharacterSet) alterItem;
                        final CharsetName charsetName = CharsetName.of(convert.getCharset());
                        if (null == charsetName || !charsetName.name().equalsIgnoreCase(convert.getCharset())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Unknown charset name '" + convert.getCharset() + "'");
                        }
                        if (convert.getCollate() != null) {
                            final CollationName collationName = CollationName.of(convert.getCollate());
                            if (null == collationName || !collationName.name().equalsIgnoreCase(convert.getCollate())) {
                                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                    "Unknown collate name '" + convert.getCollate() + "'");
                            }
                            if (!charsetName.match(collationName)) {
                                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                    "Collate name '" + convert.getCollate() + "' not support for '"
                                        + convert.getCharset() + "'");
                            }
                        }
                    }
                    break;
                default:
                    break;
                }
            }

            if (gsiExists) {
                if (alterTable.getAlters().size() > 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support multi ALTER statements on table with global secondary index");
                }

                if (alterType.belongsTo(SqlKind.ALTER_ALTER_COLUMN) && TStringUtil.isNotBlank(columnName)) {
                    // handle alter column
                    final Set<String> gsiNameByColumn = tableColumns.getGsiNameByColumn(columnName);

                    final GsiTableMetaBean gsiTableMetaBean = table.getGsiTableMetaBean();
                    for (Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                        final String indexTableName = indexEntry.getKey();

                        if (!gsiNameByColumn.contains(indexTableName)) {
                            continue;
                        }

                        if (null != alterTable.getTableOptions() && GeneralUtil.isNotEmpty(
                            alterTable.getTableOptions().getUnion())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support set table option UNION to table with global secondary index");
                        }

                        // add target table for gsi index table
                        final Map<String, List<List<String>>> targetTables =
                            buildGsiTargetTables(param, schemaName, indexTableName, ec);

                        this.gsiTargetTables.put(indexTableName, targetTables);
                    }
                } else if (alterType == SqlKind.CONVERT_TO_CHARACTER_SET) {
                    // Apply changes on all GSI table.
                    final GsiTableMetaBean gsiTableMetaBean = table.getGsiTableMetaBean();
                    for (Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                        final String indexTableName = indexEntry.getKey();

                        if (null != alterTable.getTableOptions() && GeneralUtil.isNotEmpty(
                            alterTable.getTableOptions().getUnion())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support set table option UNION to table with global secondary index");
                        }

                        // add target table for gsi index table
                        final Map<String, List<List<String>>> targetTables =
                            buildGsiTargetTables(param, schemaName, indexTableName, ec);

                        this.gsiTargetTables.put(indexTableName, targetTables);
                    }
                }
            } else if (null != alterTable.getTableOptions() && GeneralUtil.isEmpty(alterTable.getAlters())) {
                if (GeneralUtil.isEmpty(alterTable.getTableOptions().getUnion())) {
                    final GsiTableMetaBean gsiTableMetaBean = table.getGsiTableMetaBean();
                    for (Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                        final String indexTableName = indexEntry.getKey();
                        // add target table for gsi index table
                        final Map<String, List<List<String>>> targetTables =
                            buildGsiTargetTables(param, schemaName, indexTableName, ec);

                        this.gsiTargetTables.put(indexTableName, targetTables);
                    }
                }
            } else if (clusteredExists) {
                // clustered index need add column or index.
                if (alterTable.getAlters().size() > 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support multi ALTER statements on table with clustered index");
                }

                final GsiTableMetaBean gsiTableMetaBean = table.getGsiTableMetaBean();
                for (Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                    if (indexEntry.getValue().clusteredIndex) {
                        final String indexTableName = indexEntry.getKey();

                        if (null != alterTable.getTableOptions() && GeneralUtil.isNotEmpty(
                            alterTable.getTableOptions().getUnion())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support set table option UNION to table with clustered index");
                        }

                        if (alterTable.getAlters().get(0).getKind() == SqlKind.DROP_INDEX) {
                            // Special dealing.
                            final SqlAlterTableDropIndex dropIndex =
                                (SqlAlterTableDropIndex) alterTable.getAlters().get(0);
                            if (!indexExistence(indexTableName, dropIndex.getIndexName().getLastName())) {
                                continue; // Ignore this clustered index.
                            }
                        }

                        final Map<String, List<List<String>>> targetTables =
                            buildTargetTables(param, schemaName, indexTableName, ec);

                        this.gsiClusteredTargetTables.put(indexTableName, targetTables);
                    }
                }
            }
        }
    }

    private Map<String, List<List<String>>> buildTargetTables(Map<Integer, ParameterContext> param, String schemaName,
                                                              String tableName, ExecutionContext ec) {
        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        TddlRuleManager tddlRuleManager = ec.getSchemaManager(schemaName).getTddlRuleManager();
        final List<TargetDB> shard = tddlRuleManager.shard(tableName, true, true, null, param, calcParams, ec);
        List<List<TargetDB>> targetDBs = ImmutableList.of(shard);
        final Set<String> groupIntersection = getGroupIntersection(targetDBs);
        targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
        final List<Group> groups = optimizerContext.getMatrix().getGroups();

        targetDBs = fillGroup(targetDBs, groups, tableRule);
        return convertTargetDB(targetDBs);
    }

    private Map<String, List<List<String>>> buildGsiTargetTables(Map<Integer, ParameterContext> param,
                                                                 String schemaName, String tableName,
                                                                 ExecutionContext executionContext) {
        TableRule gsiTableRule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(tableName);
        TddlRuleManager tddlRuleManager = executionContext.getSchemaManager(schemaName).getTddlRuleManager();
        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        final List<TargetDB> shard =
            tddlRuleManager.shard(tableName, true, true, null, param, calcParams, executionContext);
        List<List<TargetDB>> targetDBs = ImmutableList.of(shard);
        final Set<String> groupIntersection = getGroupIntersection(targetDBs);
        targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
        final List<Group> groups = optimizerContext.getMatrix().getGroups();

        targetDBs = fillGroup(targetDBs, groups, gsiTableRule);
        return convertTargetDB(targetDBs);
    }

    static private SqlNode generateDbPartition(TableMeta tableMeta, String indexColName) {
        final ColumnMeta columnMeta =
            tableMeta.getPhysicalColumns().stream().filter(col -> col.getName().equalsIgnoreCase(indexColName))
                .findFirst().orElseThrow(() -> new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Unknown GSI column '" + indexColName + "'"));
        final String typeName = columnMeta.getField().getDataType().getStringSqlType().toLowerCase();
        return SqlValidatorImpl.assignAutoPartition(new SqlIdentifier(indexColName, SqlParserPos.ZERO), typeName);
    }

    private TableRule buildGsiTargetTableForRepartition(TableMeta tableToSchema, String indexTableName,
                                                        SqlIndexDefinition indexDef,
                                                        Map<String, Map<String, List<List<String>>>> gsiTargetTables,
                                                        ExecutionContext executionContext) {
        TableRule tableRule =
            buildTableRule(indexTableName, tableToSchema, indexDef.getDbPartitionBy(), indexDef.getDbPartitions(),
                indexDef.getTbPartitionBy(), indexDef.getTbPartitions(), ImmutableList.of(), this.optimizerContext,
                executionContext);
        if (indexDef.isBroadcast()) {
            tableRule = processBroadcast(indexTableName, tableToSchema, this.optimizerContext,
                executionContext.isRandomPhyTableEnabled());
        } else if (indexDef.isSingle()) {
            tableRule = processSingle(indexTableName, tableToSchema, this.optimizerContext,
                executionContext.isRandomPhyTableEnabled());
        }

        List<List<TargetDB>> targetDBs =
            DataNodeChooser.shardCreateTable(this.schemaName, indexTableName, this, tableRule);
        final Set<String> groupIntersection = getGroupIntersection(targetDBs);
        targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
        final List<Group> groups = optimizerContext.getMatrix().getGroups();
        targetDBs = fillGroup(targetDBs, groups, tableRule);
        if (tableRule != null && !PlannerUtils.isSingleTable(tableRule) && !tableRule.isBroadcast()) {
            this.setPartition(true);
        }
        gsiTargetTables.put(indexTableName, convertTargetDB(targetDBs));
        gsiTableRules.put(indexTableName, tableRule);
        gsiIndexDefs.put(indexTableName, indexDef);

        return tableRule;
    }

    private TableRule buildGsiTargetTable(TableMeta tableToSchema, String indexTableName, SqlIndexDefinition indexDef,
                                          Map<String, Map<String, List<List<String>>>> gsiTargetTables,
                                          ExecutionContext executionContext) {
        // Generate auto partition for clustered index.
        final SqlNode dbpartition;
        if (tableToSchema.isAutoPartition() && null == indexDef.getDbPartitionBy()) {
            final String indexColName = indexDef.getColumns().get(0).getColumnNameStr();
            dbpartition = generateDbPartition(tableToSchema, indexColName);
            // Replace the index define.
            indexDef = indexDef.rebuildToGsi(null, dbpartition);
        } else {
            dbpartition = indexDef.getDbPartitionBy();
            if (null == dbpartition) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Global (clustered) secondary index must have dbpartition.");
            }
        }

        TableRule tableRule = buildTableRule(indexTableName, tableToSchema, dbpartition, indexDef.getDbPartitions(),
            indexDef.getTbPartitionBy(), indexDef.getTbPartitions(), ImmutableList.of(), this.optimizerContext,
            executionContext);

        List<List<TargetDB>> targetDBs =
            DataNodeChooser.shardCreateTable(this.schemaName, indexTableName, this, tableRule);
        final Set<String> groupIntersection = getGroupIntersection(targetDBs);
        targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
        final List<Group> groups = optimizerContext.getMatrix().getGroups();
        targetDBs = fillGroup(targetDBs, groups, tableRule);
        if (tableRule != null && !PlannerUtils.isSingleTable(tableRule) && !tableRule.isBroadcast()) {
            this.setPartition(true);
        }
        gsiTargetTables.put(indexTableName, convertTargetDB(targetDBs));
        gsiTableRules.put(indexTableName, tableRule);
        gsiIndexDefs.put(indexTableName, indexDef);

        return tableRule;
    }

    private TableRule buildGsiTargetTable(TableMeta tableToSchema, String indexTableName, SqlCreateIndex createIndex,
                                          Map<String, Map<String, List<List<String>>>> gsiTargetTables,
                                          ExecutionContext executionContext) {
        // Generate auto partition for clustered index.
        final SqlNode dbpartition;
        if (tableToSchema.isAutoPartition() && null == createIndex.getDbPartitionBy()) {
            final String indexColName = createIndex.getColumns().get(0).getColumnNameStr();
            dbpartition = generateDbPartition(tableToSchema, indexColName);
            // Replace the index define.
            createIndex = createIndex.rebuildToGsi(null, dbpartition);
        } else {
            dbpartition = createIndex.getDbPartitionBy();
            if (null == dbpartition) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Global (clustered) secondary index must have dbpartition.");
            }
        }

        TableRule tableRule = buildTableRule(indexTableName, tableToSchema, dbpartition, createIndex.getDbPartitions(),
            createIndex.getTbPartitionBy(), createIndex.getTbPartitions(), ImmutableList.of(), this.optimizerContext,
            executionContext);

        List<List<TargetDB>> targetDBs =
            DataNodeChooser.shardCreateTable(this.schemaName, indexTableName, this, tableRule);
        final Set<String> groupIntersection = getGroupIntersection(targetDBs);
        targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
        final List<Group> groups = this.optimizerContext.getMatrix().getGroups();
        targetDBs = fillGroup(targetDBs, groups, tableRule);
        if (tableRule != null && !PlannerUtils.isSingleTable(tableRule) && !tableRule.isBroadcast()) {
            this.setPartition(true);
        }
        gsiTargetTables.put(indexTableName, convertTargetDB(targetDBs));
        gsiTableRules.put(indexTableName, tableRule);
        gsiCreateIndexs.put(indexTableName, createIndex);

        return tableRule;
    }

    public static TableRule buildTableRule(String tableName, TableMeta tableToSchema, SqlNode dbpartitionBy,
                                           SqlNode dbpartitions, SqlNode tbpartitionBy, SqlNode tbpartitions,
                                           List<MappingRule> mappingRules, OptimizerContext optimizerContext,
                                           ExecutionContext executionContext) {
        TableRule tableRule;
        DBPartitionBy dbPartitionByRule = new DBPartitionBy();
        TBPartitionBy tbPartitionByRule = new TBPartitionBy();
        DBPartitionOptions dbPartitionOptions = new DBPartitionOptions();
        if (dbpartitionBy != null) {
            final List<String> paramNames = new ArrayList<>();
            final SqlBasicCall dbFunBasicCall = (SqlBasicCall) dbpartitionBy;
            final SqlOperator operator = dbFunBasicCall.getOperator();
            String dbFunName = operator.getName();
            if (operator instanceof SqlBetweenOperator) {
                final SqlNode sqlNode = dbFunBasicCall.getOperandList().get(0);
                if (sqlNode instanceof SqlBasicCall) {
                    final SqlBasicCall sqlNode1 = (SqlBasicCall) sqlNode;
                    dbFunName = sqlNode1.getOperator().getName();
                    paramNames.add(((SqlIdentifier) sqlNode1.getOperandList().get(0)).getSimple());
                }
            } else {
                final List<SqlNode> operandList = dbFunBasicCall.getOperandList();
                for (int i = 0; i < operandList.size(); i++) {
                    final SqlNode sqlNode = operandList.get(i);
                    if (sqlNode instanceof SqlIdentifier) {
                        final String simple = ((SqlIdentifier) sqlNode).getSimple();
                        paramNames.add(simple);
                    } else if (sqlNode instanceof SqlNumericLiteral) {
                        final Object value = ((SqlNumericLiteral) sqlNode).getValue();
                        if (value instanceof BigDecimal) {
                            paramNames.add(((BigDecimal) value).toPlainString());
                        } else {
                            paramNames.add(value.toString());
                        }
                    }
                }
            }
            dbPartitionByRule.setColExpr(paramNames);
            dbPartitionByRule.setType(PartitionByType.valueOf(dbFunName.toUpperCase()));
            dbPartitionOptions.setDbpartitionBy(dbPartitionByRule);
        }
        if (dbpartitions != null) {
            Integer dbCounts = ((SqlLiteral) dbpartitions).intValue(false);
            dbPartitionOptions.setDbpartitions(dbCounts);
        }

        if (tbpartitionBy != null) {
            final List<String> paramNames = new ArrayList<>();
            final SqlBasicCall tbFunBasicCall = (SqlBasicCall) tbpartitionBy;
            final SqlOperator operator = tbFunBasicCall.getOperator();
            String tbFunName = operator.getName();
            if (operator instanceof SqlBetweenOperator) {
                final SqlNode sqlNode = tbFunBasicCall.getOperandList().get(0);
                if (sqlNode instanceof SqlBasicCall) {
                    final SqlBasicCall sqlNode1 = (SqlBasicCall) sqlNode;
                    tbFunName = sqlNode1.getOperator().getName();
                    paramNames.add(((SqlIdentifier) sqlNode1.getOperandList().get(0)).getSimple());
                }
                final List<SqlNode> operandList = tbFunBasicCall.getOperandList();
                assert operandList.size() == 3;
                final SqlNode between = operandList.get(1);
                final SqlNode and = operandList.get(2);
                if (between instanceof SqlNumericLiteral) {
                    final Object value = ((SqlNumericLiteral) between).getValue();
                    if (value instanceof BigDecimal) {
                        dbPartitionOptions.setStartWith(((BigDecimal) value).toBigInteger().intValue());
                    } else {
                        dbPartitionOptions.setStartWith(Integer.valueOf(value.toString()));
                    }
                }

                if (and instanceof SqlNumericLiteral) {
                    final Object value = ((SqlNumericLiteral) and).getValue();
                    if (value instanceof BigDecimal) {
                        dbPartitionOptions.setEndWith(((BigDecimal) value).toBigInteger().intValue());
                    } else {
                        dbPartitionOptions.setEndWith(Integer.valueOf(value.toString()));
                    }
                }
            } else {
                final List<SqlNode> operandList = tbFunBasicCall.getOperandList();
                for (int i = 0; i < operandList.size(); i++) {
                    final SqlNode sqlNode = operandList.get(i);
                    if (sqlNode instanceof SqlIdentifier) {
                        final String simple = ((SqlIdentifier) sqlNode).getSimple();
                        paramNames.add(simple);
                    } else if (sqlNode instanceof SqlNumericLiteral) {
                        final Object value = ((SqlNumericLiteral) sqlNode).getValue();
                        if (value instanceof BigDecimal) {
                            paramNames.add(((BigDecimal) value).toPlainString());
                        } else {
                            paramNames.add(value.toString());
                        }
                    }
                }
            }
            tbPartitionByRule.setColExpr(paramNames);
            tbPartitionByRule.setType(PartitionByType.valueOf(tbFunName));
            dbPartitionOptions.setTbpartitionBy(tbPartitionByRule);
        }

        if (tbpartitions != null) {
            Integer tbCounts = ((SqlLiteral) tbpartitions).intValue(false);
            dbPartitionOptions.setTbpartitions(tbCounts);
        }

        tableRule = processDBPartitionOptions(tableName, tableToSchema, dbPartitionOptions, optimizerContext,
            executionContext.isRandomPhyTableEnabled());

        // 热点映射
        if (mappingRules != null && mappingRules.size() > 0) {
            tableRule.setExtPartitions(mappingRules);
            tableRule.initExtTopology();

            for (Object dbRule : tableRule.getDbShardRules()) {
                if (!(dbRule instanceof WrappedGroovyRule)) {
                    throw new UnsupportedOperationException("Not supported db rule type for hot mapping");
                }
            }

            if (tableRule.getTbShardRules() != null) {
                for (Object tbRule : tableRule.getTbShardRules()) {
                    if (!(tbRule instanceof WrappedGroovyRule)) {
                        throw new UnsupportedOperationException("Not supported table rule type for hot mapping");
                    }
                }
            }

            if (!partitionByTypeAllowedInHotMapping(dbPartitionByRule.getType()) || !partitionByTypeAllowedInHotMapping(
                tbPartitionByRule.getType())) {
                throw new UnsupportedOperationException("Not suppored partition type for hot mapping");
            }
        }
        return tableRule;
    }

    public List<RelNode> getInput(ExecutionContext executionContext) {
        Map<Integer, ParameterContext> param = executionContext.getParamMap();
        if (targetInput == null) {
            Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
            PhyDDLViewBuilder phyTableScanbuilder =
                new PhyDDLViewBuilder(getSqlTemplate(), targetTables, param, this, DbType.MYSQL, this.schemaName,
                    executionContext);
            targetInput = phyTableScanbuilder.build();
        }
        return targetInput;
    }

    public List<RelNode> getInputAddGsi(ExecutionContext executionContext, Map<String, List<RelNode>> gsiTargetInputs) {
        Map<Integer, ParameterContext> param = executionContext.getParamMap();
        if (targetInput == null) {
            final TableMeta mainTableSchema = optimizerContext.getLatestSchemaManager().getTable(getLogicalTableName());
            final String indexName;
            if (sqlNode instanceof SqlCreateIndex) {
                indexName = ((SqlCreateIndex) sqlNode).getIndexName().getLastName();
            } else {
                indexName = ((SqlAddIndex) ((SqlAlterTable) sqlNode).getAlters().get(0)).getIndexName().getLastName();
            }

            if (mainTableSchema.isAutoPartition()) {
                Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);

                SqlNode originalSqlNode = sqlNode;

                final String localIndexNameString = TddlConstants.AUTO_LOCAL_INDEX_PREFIX + indexName;
                final SqlIdentifier localIndexName = new SqlIdentifier(localIndexNameString, SqlParserPos.ZERO);
                final SqlNode fakeNode;
                if (originalSqlNode instanceof SqlCreateIndex) {
                    final String orgSql = ((SqlCreateIndex) originalSqlNode).getSourceSql();
                    final List<SQLStatement> stmts =
                        SQLUtils.parseStatementsWithDefaultFeatures(orgSql, JdbcConstants.MYSQL);
                    final SQLCreateIndexStatement stmt = ((SQLCreateIndexStatement) stmts.get(0));
                    stmt.setGlobal(false);
                    stmt.setClustered(false);
                    stmt.getIndexDefinition()
                        .setName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(localIndexNameString)));

                    // Fake one.
                    sqlNode = fakeNode =
                        ((SqlCreateIndex) originalSqlNode).rebuildToExplicitLocal(localIndexName, stmt.toString());
                } else {
                    final SqlAlterTable alterTable = ((SqlAlterTable) originalSqlNode);
                    final String orgSql = alterTable.getSourceSql();
                    final List<SQLStatement> stmts =
                        SQLUtils.parseStatementsWithDefaultFeatures(orgSql, JdbcConstants.MYSQL);
                    final SQLAlterTableStatement stmt = ((SQLAlterTableStatement) stmts.get(0));
                    if (stmt.getItems().get(0) instanceof SQLAlterTableAddIndex) {
                        ((SQLAlterTableAddIndex) stmt.getItems().get(0)).getIndexDefinition().setGlobal(false);
                        ((SQLAlterTableAddIndex) stmt.getItems().get(0)).getIndexDefinition().setClustered(false);
                        ((SQLAlterTableAddIndex) stmt.getItems().get(0)).getIndexDefinition()
                            .setName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(localIndexNameString)));
                    } else if (stmt.getItems().get(0) instanceof SQLAlterTableAddConstraint
                        && ((SQLAlterTableAddConstraint) stmt.getItems().get(0)).getConstraint() instanceof SQLUnique) {
                        final SQLIndexDefinition indexDefinition =
                            ((SQLUnique) ((SQLAlterTableAddConstraint) stmt.getItems()
                                .get(0)).getConstraint()).getIndexDefinition();
                        indexDefinition.setGlobal(false);
                        indexDefinition.setClustered(false);
                        indexDefinition.setName(
                            new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(localIndexNameString)));
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Unknown create GSI.");
                    }

                    // Fake one.
                    final SqlAddIndex addIndex = ((SqlAddIndex) ((SqlAlterTable) originalSqlNode).getAlters().get(0));
                    sqlNode = fakeNode =
                        new SqlAlterTable(null, alterTable.getOriginTableName(), alterTable.getColumnOpts(),
                            stmt.toString(), alterTable.getTableOptions(), ImmutableList.of(
                            new SqlAddIndex(SqlParserPos.ZERO, localIndexName,
                                addIndex.getIndexDef().rebuildToExplicitLocal(localIndexName))), SqlParserPos.ZERO);
                }

                final SqlNode localIndexTemplate = getSqlTemplate();
                PhyDDLViewBuilder phyTableScanbuilder =
                    new PhyDDLViewBuilder(localIndexTemplate, targetTables, param, this, DbType.MYSQL, this.schemaName,
                        executionContext);
                targetInput = phyTableScanbuilder.build(); // This for adding local on primary table.

                // Create table for GSI first.
                sqlNode = originalSqlNode;
                final Map<String, List<List<String>>> targetTablesForAddGSI = gsiTargetTables.get(indexName);
                PhyDDLViewBuilder gsiPhyTableScanbuilder =
                    new PhyDDLViewBuilder(getSqlTemplate(), // This will convert to create table.
                        targetTablesForAddGSI, param, this, DbType.MYSQL, this.schemaName, executionContext);
                gsiTargetInputs.put(indexName, gsiPhyTableScanbuilder.build());

                // This for adding local on clustered.
                originalSqlNode = sqlNode;
                sqlNode = fakeNode;
                for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiClusteredTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();

                    final ReplaceTableNameWithQuestionMarkVisitor visitor =
                        new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
                    if (localIndexTemplate instanceof SqlCreateIndex) {
                        buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables,
                            ((SqlCreateIndex) localIndexTemplate).replaceTableName(
                                new SqlIdentifier(indexTableName, SqlParserPos.ZERO)).accept(visitor),
                            executionContext);
                    } else {
                        buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables,
                            ((SqlAlterTable) localIndexTemplate).replaceTableName(
                                new SqlIdentifier(indexTableName, SqlParserPos.ZERO)).accept(visitor),
                            executionContext);
                    }
                }

                // Restore sql node.
                sqlNode = originalSqlNode;
            } else {
                // Not auto partition table.
                getTargetTables(executionContext);
                Map<String, List<List<String>>> targetTables = gsiTargetTables.get(indexName);
                PhyDDLViewBuilder phyTableScanbuilder =
                    new PhyDDLViewBuilder(getSqlTemplate(), targetTables, param, this, DbType.MYSQL, this.schemaName,
                        executionContext);
                targetInput = phyTableScanbuilder.build();
            }
        }
        return targetInput;
    }

    public void buildGsiInput(Map<Integer, ParameterContext> param, Map<String, List<RelNode>> gsiTargetInputs,
                              String indexTableName, Map<String, List<List<String>>> indexTargetTables, SqlNode sqlNode,
                              ExecutionContext ec) {
        final PhyDDLViewBuilder ddlViewBuilder =
            new PhyDDLViewBuilder(sqlNode, indexTargetTables, param, this, DbType.MYSQL, this.schemaName, ec);
        ddlViewBuilder.setIndexTableName(indexTableName);
        ddlViewBuilder.setDdlOnGsiTable(true);

        final List<RelNode> gsiTargetInput = ddlViewBuilder.build();

        gsiTargetInputs.put(indexTableName, gsiTargetInput);
        this.gsiTargetInputs.put(indexTableName, gsiTargetInput);
        this.gsiParentSqlNode.put(indexTableName, sqlNode);
    }

    public List<RelNode> getInputRenameGsi(ExecutionContext executionContext,
                                           Map<String, List<RelNode>> gsiTargetInputs) {

        Map<Integer, ParameterContext> param = executionContext.getParamMap();
        if (targetInput == null) {
            Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
            PhyDDLViewBuilder phyTableScanbuilder =
                new PhyDDLViewBuilder(getSqlTemplate(), targetTables, param, this, DbType.MYSQL, this.schemaName,
                    executionContext);
            targetInput = phyTableScanbuilder.build();

            if (null != gsiTargetInputs && GeneralUtil.isNotEmpty(this.gsiTargetTables)) {
                final SqlAlterTable alterTable = (SqlAlterTable) getNativeSqlNode();
                final SqlAlterTableRenameIndex renameIndex = (SqlAlterTableRenameIndex) alterTable.getAlters().get(0);
                // global secondary index
                for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();
                    final SqlIdentifier indexName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);

                    final SqlIdentifier newIndexName = renameIndex.getNewIndexName();
                    SqlRenameTable renameTable =
                        SqlDdlNodes.renameTable(newIndexName, indexName, "", SqlParserPos.ZERO);
                    renameTable.setSourceSql(renameTable.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql());

                    final ReplaceTableNameWithQuestionMarkVisitor visitor =
                        new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
                    renameTable = (SqlRenameTable) renameTable.accept(visitor);

                    this.gsiNewIndexName.put(indexTableName, newIndexName);

                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, renameTable,
                        executionContext);
                }
            }
        }
        return targetInput;
    }

    public List<RelNode> getInputCreateIndex(ExecutionContext executionContext,
                                             Map<String, List<RelNode>> gsiTargetInputs) {
        Map<Integer, ParameterContext> param = executionContext.getParamMap();
        if (targetInput == null) {
            Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
            PhyDDLViewBuilder phyTableScanbuilder =
                new PhyDDLViewBuilder(getSqlTemplate(), targetTables, param, this, DbType.MYSQL, this.schemaName,
                    executionContext);
            targetInput = phyTableScanbuilder.build();

            // Create local index. Dealing on clustered index.
            if (sqlNode instanceof SqlCreateIndex) {
                for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiClusteredTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();

                    final SqlCreateIndex createIndex = (SqlCreateIndex) getNativeSqlNode();
                    final ReplaceTableNameWithQuestionMarkVisitor visitor =
                        new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
                    final SqlCreateIndex newCreateIndex = (SqlCreateIndex) createIndex.replaceTableName(
                        new SqlIdentifier(indexTableName, SqlParserPos.ZERO)).accept(visitor);

                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, newCreateIndex,
                        executionContext);
                }
            }
        }
        return targetInput;
    }

    public List<RelNode> getInputDropGsi(ExecutionContext executionContext,
                                         Map<String, List<RelNode>> gsiTargetInputs) {
        Map<Integer, ParameterContext> param = executionContext.getParamMap();
        if (targetInput == null) {
            Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
            PhyDDLViewBuilder phyTableScanbuilder =
                new PhyDDLViewBuilder(getSqlTemplate(), targetTables, param, this, DbType.MYSQL, this.schemaName,
                    executionContext);
            targetInput = phyTableScanbuilder.build();

            if (null != gsiTargetInputs && GeneralUtil.isNotEmpty(this.gsiTargetTables)) {
                // global secondary index
                for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();
                    final SqlIdentifier indexName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);

                    final ReplaceTableNameWithQuestionMarkVisitor visitor =
                        new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
                    SqlDropTable dropTable = SqlDdlNodes.dropTable(SqlParserPos.ZERO, true, indexName, true);
                    dropTable = (SqlDropTable) dropTable.accept(visitor);

                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, dropTable,
                        executionContext);
                }
            }

            if (sqlNode.getKind() != SqlKind.DROP_TABLE) {
                // Drop index or alter table drop index.
                final String indexName;
                if (sqlNode instanceof SqlDropIndex) {
                    indexName = ((SqlDropIndex) sqlNode).getIndexName().getLastName();
                } else if (sqlNode instanceof SqlAlterTable) {
                    assert sqlNode.getKind() == SqlKind.ALTER_TABLE;
                    final SqlAlterTable alterTable = (SqlAlterTable) sqlNode;
                    if (alterTable.getAlters().size() > 1) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support multi ALTER statements on table with clustered index");
                    }
                    assert alterTable.getAlters().get(0).getKind() == SqlKind.DROP_INDEX;
                    indexName = ((SqlAlterTableDropIndex) alterTable.getAlters().get(0)).getIndexName().getLastName();
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Unknown drop index stmt.");
                }

                if (gsiTargetInputs != null && GeneralUtil.isEmpty(gsiTargetInputs)) {
                    // Drop local index. Dealing on clustered index.
                    for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiClusteredTargetTables
                        .entrySet()) {
                        final String indexTableName = entry.getKey();

                        if (!indexExistence(indexTableName, indexName)) {
                            continue; // Ignore if not existence.
                        }

                        final Map<String, List<List<String>>> indexTargetTables = entry.getValue();

                        final ReplaceTableNameWithQuestionMarkVisitor visitor =
                            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
                        final SqlIdentifier newTableName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);
                        if (sqlNode instanceof SqlAlterTable) {
                            final SqlAlterTable alterTable = (SqlAlterTable) getNativeSqlNode();
                            final SqlAlterTable newAlterTable =
                                (SqlAlterTable) alterTable.replaceTableName(newTableName).accept(visitor);

                            buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, newAlterTable,
                                executionContext);
                        } else {
                            final SqlDropIndex dropIndex = (SqlDropIndex) sqlNode;
                            final SqlDropIndex newDropIndex =
                                (SqlDropIndex) dropIndex.replaceTableName(newTableName).accept(visitor);

                            buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, newDropIndex,
                                executionContext);
                        }
                    }
                } else if (gsiTargetInputs != null) {
                    // Drop GSI.
                    final TableMeta mainTableSchema =
                        optimizerContext.getLatestSchemaManager().getTable(getLogicalTableName());
                    if (mainTableSchema.isAutoPartition()) {
                        final String generatedLocalIndexNameString = TddlConstants.AUTO_LOCAL_INDEX_PREFIX + indexName;
                        final SqlIdentifier generatedLocalIndexName =
                            new SqlIdentifier(generatedLocalIndexNameString, SqlParserPos.ZERO);

                        // Partition table drop index and local index.
                        for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiClusteredTargetTables
                            .entrySet()) {
                            final String indexTableName = entry.getKey();
                            if (!indexExistence(indexTableName, generatedLocalIndexNameString)) {
                                continue;
                            }

                            final Map<String, List<List<String>>> indexTargetTables = entry.getValue();

                            final ReplaceTableNameWithQuestionMarkVisitor visitor =
                                new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
                            final String dropIndexSql =
                                "DROP INDEX " + SqlIdentifier.surroundWithBacktick(generatedLocalIndexNameString)
                                    + " ON " + SqlIdentifier.surroundWithBacktick(schemaName) + "."
                                    + SqlIdentifier.surroundWithBacktick(indexTableName);
                            SqlDropIndex dropIndex = SqlDdlNodes.dropIndex(generatedLocalIndexName,
                                new SqlIdentifier(indexTableName, SqlParserPos.ZERO), dropIndexSql, SqlParserPos.ZERO);
                            dropIndex = (SqlDropIndex) dropIndex.accept(visitor);

                            buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, dropIndex,
                                executionContext);
                        }

                        if (indexExistence(getLogicalTableName(), generatedLocalIndexNameString)) {
                            // Rebuild primary on generated key.
                            final ReplaceTableNameWithQuestionMarkVisitor visitor =
                                new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
                            final String dropIndexSql =
                                "DROP INDEX " + SqlIdentifier.surroundWithBacktick(generatedLocalIndexNameString)
                                    + " ON " + SqlIdentifier.surroundWithBacktick(schemaName) + "."
                                    + SqlIdentifier.surroundWithBacktick(getLogicalTableName());
                            SqlDropIndex dropIndex = SqlDdlNodes.dropIndex(generatedLocalIndexName,
                                new SqlIdentifier(getLogicalTableName(), SqlParserPos.ZERO), dropIndexSql,
                                SqlParserPos.ZERO);
                            dropIndex = (SqlDropIndex) dropIndex.accept(visitor);
                            PhyDDLViewBuilder phyTableScanbuilder2 =
                                new PhyDDLViewBuilder(dropIndex, targetTables, param, this, DbType.MYSQL,
                                    this.schemaName, executionContext);
                            targetInput = phyTableScanbuilder2.build();
                        } else {
                            targetInput.clear();
                        }
                    } else {
                        // GSI on normal table. Just drop GSI.
                        targetInput.clear();
                    }
                }
            }
        }
        return targetInput;
    }

    public List<RelNode> getInputTruncateGsi(ExecutionContext executionContext,
                                             Map<String, List<RelNode>> gsiTargetInputs) {
        Map<Integer, ParameterContext> param = executionContext.getParamMap();
        if (targetInput == null) {
            Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
            PhyDDLViewBuilder phyTableScanbuilder =
                new PhyDDLViewBuilder(getSqlTemplate(), targetTables, param, this, DbType.MYSQL, this.schemaName,
                    executionContext);
            targetInput = phyTableScanbuilder.build();

            if (null != gsiTargetInputs && GeneralUtil.isNotEmpty(this.gsiTargetTables)) {
                // global secondary index
                for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();
                    final SqlIdentifier indexName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);

                    final ReplaceTableNameWithQuestionMarkVisitor visitor =
                        new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
                    SqlTruncateTable truncateTable =
                        SqlDdlNodes.truncateTable(SqlParserPos.ZERO, false, indexName, true);
                    truncateTable = (SqlTruncateTable) truncateTable.accept(visitor);

                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, truncateTable,
                        executionContext);
                }

            }
        }
        return targetInput;
    }

    public boolean needRewriteToGsi(boolean rewrite) {
        // Rewrite create index and alter table add index.
        switch (sqlNode.getKind()) {
        case CREATE_INDEX: {
            final TableMeta tableMeta = optimizerContext.getLatestSchemaManager().getTable(getLogicalTableName());
            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName) && tableMeta.isAutoPartition()) {
                // Legacy code. (auto partition on sharding table do rewrite here)
                final SqlCreateIndex createIndex = (SqlCreateIndex) sqlNode;
                if (null == createIndex.getIndexResiding()) {
                    // Need rewrite.
                    if (rewrite) {
                        sqlNode = createIndex.rebuildToGsi(null, null);
                    }
                    return true;
                }
            }
        }
        break;

        case ALTER_TABLE: {
            final TableMeta tableMeta = optimizerContext.getLatestSchemaManager().getTable(getLogicalTableName());
            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName) && tableMeta.isAutoPartition()) {
                // Legacy code. (auto partition on sharding table do rewrite here)
                final SqlAlterTable alterTable = (SqlAlterTable) sqlNode;
                for (int idx = 0; idx < alterTable.getAlters().size(); ++idx) {
                    final SqlAlterSpecification specification = alterTable.getAlters().get(idx);
                    if (specification instanceof SqlAddIndex) {
                        final SqlAddIndex addIndex = (SqlAddIndex) specification;
                        if (!addIndex.getIndexDef().isClustered() && !addIndex.getIndexDef().isGlobal()
                            && !addIndex.getIndexDef().isLocal()) {
                            // Need rewrite.
                            if (rewrite) {
                                alterTable.getAlters().set(idx,
                                    new SqlAddIndex(addIndex.getParserPosition(), addIndex.getIndexName(),
                                        addIndex.getIndexDef().rebuildToGsi(null, null)));
                            }
                            return true;
                        }
                    }
                }
            }
        }
        break;

        default:
            break;
        }
        return false;
    }

    public List<RelNode> getInputAlterGsi(ExecutionContext executionContext,
                                          Map<String, List<RelNode>> gsiTargetInputs) {
        Map<Integer, ParameterContext> param = executionContext.getParamMap();
        if (targetInput == null) {
            Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
            PhyDDLViewBuilder phyTableScanbuilder =
                new PhyDDLViewBuilder(getSqlTemplate(), targetTables, param, this, DbType.MYSQL, this.schemaName,
                    executionContext);
            targetInput = phyTableScanbuilder.build();

            if (null != gsiTargetInputs && GeneralUtil.isNotEmpty(this.gsiTargetTables)) {
                // global secondary index
                for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();

                    final SqlAlterTable originAlterTable = (SqlAlterTable) getNativeSqlNode();
                    final ReplaceTableNameWithQuestionMarkVisitor visitor =
                        new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
                    final SqlAlterTable alterTable =
                        (SqlAlterTable) originAlterTable.replaceTableName(new SqlIdentifier(indexTableName,
                                SqlParserPos.ZERO))
                            .accept(visitor);

                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, alterTable,
                        executionContext);
                }
            }

            if (null != gsiTargetInputs && GeneralUtil.isNotEmpty(this.gsiClusteredTargetTables)) {
                for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiClusteredTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();

                    final SqlAlterTable originAlterTable = (SqlAlterTable) getNativeSqlNode();
                    final ReplaceTableNameWithQuestionMarkVisitor visitor =
                        new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
                    final SqlAlterTable alterTable =
                        (SqlAlterTable) originAlterTable.replaceTableName(new SqlIdentifier(indexTableName,
                                SqlParserPos.ZERO))
                            .accept(visitor);

                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, alterTable,
                        executionContext);
                }
            }
        }
        return targetInput;
    }

    public List<RelNode> getInputCreateGsi(ExecutionContext executionContext,
                                           Map<String, List<RelNode>> gsiTargetInputs) {
        Map<Integer, ParameterContext> param = executionContext.getParamMap();
        if (targetInput == null) {
            Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
            PhyDDLViewBuilder phyTableScanbuilder =
                new PhyDDLViewBuilder(getSqlTemplate(), targetTables, param, this, DbType.MYSQL, this.schemaName,
                    executionContext);
            targetInput = phyTableScanbuilder.build();

            if (null != gsiTargetInputs && (GeneralUtil.isNotEmpty(this.gsiTargetTables) || GeneralUtil.isNotEmpty(
                gusiTargetTables) || GeneralUtil.isNotEmpty(this.gsiClusteredTargetTables) || GeneralUtil.isNotEmpty(
                gusiClusteredTargetTables))) {
                final SqlCreateTable createTable = (SqlCreateTable) getSqlTemplate();
                final String mainTableDefinition = createTable.rewriteForGsi().toString();
                // global secondary index
                for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();
                    final Map<SqlAlterTable.ColumnOpt, List<String>> columnOpts = new HashMap<>();
                    final SqlTableOptions tableOptions = null;
                    final List<SqlAlterSpecification> alters = new ArrayList<>();
                    final SqlIdentifier indexName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);
                    final SqlIndexDefinition indexDef = gsiIndexDefs.get(indexTableName);
                    final SqlIdentifier tableName = new SqlIdentifier(Util.last(tableNames), SqlParserPos.ZERO);

                    alters.add(new SqlAddIndex(SqlParserPos.ZERO, indexName, indexDef));
                    indexDef.setPrimaryTableDefinition(mainTableDefinition);

                    final SqlAlterTable addIndex =
                        new SqlAlterTable(null, tableName, columnOpts, "", tableOptions, alters, SqlParserPos.ZERO);

                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, addIndex,
                        executionContext);
                }

                // global unique secondary index
                for (Entry<String, Map<String, List<List<String>>>> entry : this.gusiTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();
                    final Map<SqlAlterTable.ColumnOpt, List<String>> columnOpts = new HashMap<>();
                    final SqlTableOptions tableOptions = null;
                    final List<SqlAlterSpecification> alters = new ArrayList<>();
                    final SqlIdentifier indexName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);
                    final SqlIndexDefinition indexDef = gsiIndexDefs.get(indexTableName);
                    final SqlIdentifier tableName = new SqlIdentifier(Util.last(tableNames), SqlParserPos.ZERO);

                    alters.add(new SqlAddUniqueIndex(SqlParserPos.ZERO, indexName, indexDef));
                    indexDef.setPrimaryTableDefinition(mainTableDefinition);

                    final SqlAlterTable addIndex =
                        new SqlAlterTable(null, tableName, columnOpts, "", tableOptions, alters, SqlParserPos.ZERO);

                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, addIndex,
                        executionContext);
                }

                for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiClusteredTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();
                    final Map<SqlAlterTable.ColumnOpt, List<String>> columnOpts = new HashMap<>();
                    final SqlTableOptions tableOptions = null;
                    final List<SqlAlterSpecification> alters = new ArrayList<>();
                    final SqlIdentifier indexName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);
                    final SqlIndexDefinition indexDef = gsiIndexDefs.get(indexTableName);
                    final SqlIdentifier tableName = new SqlIdentifier(Util.last(tableNames), SqlParserPos.ZERO);

                    alters.add(new SqlAddIndex(SqlParserPos.ZERO, indexName, indexDef));
                    indexDef.setPrimaryTableDefinition(mainTableDefinition);

                    final SqlAlterTable addIndex =
                        new SqlAlterTable(null, tableName, columnOpts, "", tableOptions, alters, SqlParserPos.ZERO);

                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, addIndex,
                        executionContext);
                }

                for (Entry<String, Map<String, List<List<String>>>> entry : this.gusiClusteredTargetTables.entrySet()) {
                    final String indexTableName = entry.getKey();
                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();
                    final Map<SqlAlterTable.ColumnOpt, List<String>> columnOpts = new HashMap<>();
                    final SqlTableOptions tableOptions = null;
                    final List<SqlAlterSpecification> alters = new ArrayList<>();
                    final SqlIdentifier indexName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);
                    final SqlIndexDefinition indexDef = gsiIndexDefs.get(indexTableName);
                    final SqlIdentifier tableName = new SqlIdentifier(Util.last(tableNames), SqlParserPos.ZERO);

                    alters.add(new SqlAddUniqueIndex(SqlParserPos.ZERO, indexName, indexDef));
                    indexDef.setPrimaryTableDefinition(mainTableDefinition);

                    final SqlAlterTable addIndex =
                        new SqlAlterTable(null, tableName, columnOpts, "", tableOptions, alters, SqlParserPos.ZERO);

                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, addIndex,
                        executionContext);
                }
            }
        }
        return targetInput;
    }

//    public List<RelNode> getInputDropPartition(ExecutionContext executionContext) {
//        Map<Integer, ParameterContext> param =
//            executionContext.getParams() == null ? null : executionContext.getParams()
//                .getCurrentParameter();
//        if (targetInput == null) {
//            Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
//            PhyDDLViewBuilder phyTableScanbuilder = new PhyDDLViewBuilder(getSqlTemplate(),
//                targetTables,
//                param,
//                this,
//                DbType.MYSQL,
//                this.schemaName);
//            targetInput = phyTableScanbuilder.build();
//
//            if (null != gsiTargetInputs && GeneralUtil.isNotEmpty(this.gsiTargetTables)) {
//                // global secondary index
//                for (Entry<String, Map<String, List<List<String>>>> entry : this.gsiTargetTables.entrySet()) {
//                    final String indexTableName = entry.getKey();
//                    final Map<String, List<List<String>>> indexTargetTables = entry.getValue();
//                    final SqlIdentifier indexName = new SqlIdentifier(indexTableName, SqlParserPos.ZERO);
//
//                    final ReplaceTableNameWithQuestionMarkVisitor visitor =
//                        new ReplaceTableNameWithQuestionMarkVisitor(schemaName);
//                    SqlDropTable dropTable = SqlDdlNodes.dropTable(SqlParserPos.ZERO, true, indexName, true);
//                    dropTable = (SqlDropTable) dropTable.accept(visitor);
//
//                    buildGsiInput(param, gsiTargetInputs, indexTableName, indexTargetTables, dropTable);
//                }
//
//            }
//        }
//        return targetInput;
//    }

    public SqlNode getSqlTemplate() {
        return buildSqlTemplate();
    }

    protected SqlNode buildSqlTemplate() {
        SqlNode sqlTemplate = getNativeSqlNode();
        ReplaceTableNameWithQuestionMarkVisitor visitor = new ReplaceTableNameWithQuestionMarkVisitor(schemaName,
            PlannerContext.getPlannerContext(this).getExecutionContext());
        return sqlTemplate.accept(visitor);
    }

    public SqlNode getNativeSqlNode() {
        return this.sqlNode;
    }

    public void setNativeSqlNode(SqlNode newSqlNode) {
        this.sqlNode = newSqlNode;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        // We need Parameters to get routing result.
        Parameters parameterSettings = null;
        ExecutionContext executionContext = null;
        if (pw instanceof RelDrdsWriter) {
            Map<Integer, ParameterContext> params = ((RelDrdsWriter) pw).getParams();
            if (params != null) {
                parameterSettings = new Parameters(params, false);
            }
            executionContext = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        }
        if (executionContext == null) {
            executionContext = new ExecutionContext();
            if (schemaName != null) {
                executionContext.setSchemaName(schemaName);
            }
        }
        executionContext.setParams(parameterSettings);

        if (input instanceof LogicalValues || input instanceof LogicalDynamicValues) {
            List<RelNode> inputs = getInput(executionContext, null);
            for (RelNode input : inputs) {
                input.explainForDisplay(pw);
            }
        } else {
            // insert select
            pw.item(RelDrdsWriter.REL_NAME, explainNodeName());
            pw.item("table", getLogicalTableName());
            pw.item("columns", rowType);
        }

        return pw;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public String getLogicalTableName() {
        if (getTableName() instanceof SqlIdentifier) {
            return Util.last(((SqlIdentifier) getTableName()).names);
        } else {
            return "?";
        }
    }

    public void setTableName(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        final RelNode relNode = inputs.get(0);
        return new DataDefLanguageLogicView(this.getCluster(), traitSet, relNode, this.sqlNode, getTableName());
    }

    public String explainNodeName() {
        return "DDL " + getOperation();
    }

    @Override
    public List<RelNode> getInputs() {
        if (input == null) {
            return ImmutableList.of();
        }
        return ImmutableList.of(input);
    }

    private void fillRenamePhyTable(String schemaName, Map<String, List<List<String>>> targetDBs, String toTableName,
                                    String logicalTableName) {
        final Set<String> logicalNames = targetDBs.keySet();
        for (String logName : logicalNames) {
            final List<List<String>> lists = targetDBs.get(logName);
            for (List<String> phyList : lists) {
                final String tableName =
                    genTargetActualTableName(schemaName, logicalTableName, toTableName, phyList.get(0));
                phyList.add(tableName);
            }
        }
    }

    public static TableRule processBroadcast(String tableName, TableMeta tableMeta) {
        return processBroadcast(tableName, tableMeta, OptimizerContext.getContext(tableMeta.getSchemaName()), true);
    }

    public static TableRule processBroadcastWithoutRandomPhyTableName(String tableName, TableMeta tableMeta) {
        return processBroadcast(tableName, tableMeta, OptimizerContext.getContext(tableMeta.getSchemaName()), false);
    }

    public static TableRule processSingle(String tableName, TableMeta tableMeta, OptimizerContext optimizerContext,
                                          boolean randomPhyTableNameEnabled) {

        String defaultDb = optimizerContext.getRuleManager().getDefaultDbIndex(null);

        TableRule tableRule = new TableRule();
        tableRule.setRandomTableNamePatternEnabled(randomPhyTableNameEnabled);

        if (randomPhyTableNameEnabled) {
            tableName = RuleUtils.genTableNameWithRandomSuffix(tableRule, tableName);
        }

        tableRule.setDbNamePattern(defaultDb);
        tableRule.setTbNamePattern(tableName);
        tableRule.init();
        return tableRule;
    }

    /**
     * 处理广播表情形
     */
    private static TableRule processBroadcast(String tableName, TableMeta tableMeta, OptimizerContext optimizerContext,
                                              boolean randomPhyTableNameEnabled) {
        TableRule tableRule = new TableRule();

        tableRule.setBroadcast(true);
        tableRule.setRandomTableNamePatternEnabled(randomPhyTableNameEnabled);

        int table_count_on_each_group = 1; /* 一定单表 */
        int group_count = 1; /* 广播规则生成只需要1 */

        String defaultDb = optimizerContext.getRuleManager().getDefaultDbIndex(null);

        /* 用于检查 */
        List<String> selGroupList = new ArrayList<String>();
        tableRule.setDbNamePattern(defaultDb);
        /* 这里需要跳过后面的检测 */
        selGroupList.add(defaultDb);

        populateExistingRandomSuffix(tableName, tableRule, optimizerContext, randomPhyTableNameEnabled);

        /* 借用HASH的partition/subpartition生成法则 */
        IPartitionGen partitionGen = TableRuleGenFactory.getInstance()
            .createDBPartitionGenerator(optimizerContext.getSchemaName(), PartitionByType.HASH);
        ISubpartitionGen subpartitionGen = TableRuleGenFactory.getInstance()
            .createTBPartitionGenerator(optimizerContext.getSchemaName(), PartitionByType.HASH, PartitionByType.HASH);

        List<String> partitionParams = new ArrayList<String>();
        partitionGen.fillDbRuleStandAlone(tableRule, partitionParams, // 分库键
            tableMeta, group_count, // 分库数
            1, null);

        subpartitionGen.fillTbRuleStandAlone(tableRule, partitionParams, // 分表键
            tableMeta, group_count, table_count_on_each_group, tableName, null);

        /* 总是加上allowfulltablescan */
        tableRule.setAllowFullTableScan(true);

        /**
         * 这里可以直接做规则推导,因为来源是从SQL来的不存在 并发竞争的问题.
         */
        tableRule.init();

        /**
         * 这里特别的有一种情况，因为前面是通过真实的group列表来生成tableRule的
         * 但这里是通过这个抽象的tableRule来枚举出计算出的group列表的，这就要求
         * 真实的group必须是连续的，否则如果真实的是0,1,3,4，我这里计算的结果则会是
         * 0,1,2,3就会出错，而且这种错误是在执行的时候才会发现的，而且即使我在这里可以将
         * 真实的groupList带过来，但是之行的时候还是会shard到不存在的group中，这样还不如在
         * 建库的时候直接报错比较好，所以此处需要做预先校验工作
         */
        Map<String, Set<String>> topology = tableRule.getActualTopology();

        /* 只要选择的group能包含所有的推演出的group就代表OK */
        if (!RuleUtils.checkIfGroupMatch(new HashSet<String>(selGroupList), topology.keySet())) {
            throw new IllegalArgumentException("Selected physical group list is invalid:" + selGroupList);
        }

        /**
         * 因为前面的各种方式目的就是保证所有分表编号全局唯一，所以 这里进行最后的检查
         */
        String dbRule = (tableRule.getDbRuleStrs() == null || tableRule.getDbRuleStrs().length == 0) ? null :
            tableRule.getDbRuleStrs()[0];
        String tbRule = (tableRule.getTbRulesStrs() == null || tableRule.getTbRulesStrs().length == 0) ? null :
            tableRule.getTbRulesStrs()[0];

        /**
         * 检查最终生成的分表数与期望分表数是否相同
         */
        if (!RuleUtils.checkIfTableNumberOk(topology, group_count, table_count_on_each_group)) {
            throw new IllegalArgumentException(
                "Generated table number mismatch" + " dbName: " + tableRule.getDbNamePattern() + " tbName: "
                    + tableRule.getTbNamePattern() + " dbRule: " + dbRule + " tbRule: " + tbRule);
        }

        return tableRule;
    }

    /**
     * Convert processDBPartitionOptions to tableRuleList partitions是总group数
     * subpartitoins是每个group的表数，而不是总表数，这样方便使null和1相等
     */
    public static TableRule processDBPartitionOptions(String tableName, TableMeta tableMeta,
                                                      DBPartitionOptions dbpartitionOptions,
                                                      OptimizerContext optimizerContext,
                                                      boolean randomPhyTableNameEnabled) {
        if (dbpartitionOptions == null) {
            /* 无dbpartition为单库单表 */
            return null;
        }

        DBPartitionBy dbpartitionBy = dbpartitionOptions.getDbpartitionBy();
        TBPartitionBy tbpartitionBy = dbpartitionOptions.getTbpartitionBy();
        List<DBPartitionDefinition> dbpartitionDefinitionList = dbpartitionOptions.getDbpartitionDefinitionList();

        DBPartitionDefinition dbpartitionDefinition = null;
        if (dbpartitionDefinitionList != null && dbpartitionDefinitionList.size() > 0) {
            /* only take item(0) of partitionDefinitionList */
            dbpartitionDefinition = dbpartitionDefinitionList.get(0);
        }
        TBPartitionDefinition tbpartitionDefinition = null;
        if (dbpartitionDefinition != null && dbpartitionDefinition.getTbpartitionDefinitionList() != null
            && dbpartitionDefinition.getTbpartitionDefinitionList().size() > 0) {
            /* only take item(0) of SubpartitionDefinitionList */
            tbpartitionDefinition = dbpartitionDefinition.getTbpartitionDefinitionList().get(0);
        }

        TableRule tableRule = new TableRule();
        boolean needCheckTable = true;
        /**
         * 判断partition by,如果为NULL，则没有规则，而是SQL直接下推到缺省DB Statement
         * node的PartitionBy作为后面是否存在partition部分的直接依据，也就是说不允许
         * 没有Partition的Subpartition关键字的形式。
         */
        if (dbpartitionBy == null && tbpartitionBy == null) {
            return null;
        }

        tableRule.setRandomTableNamePatternEnabled(randomPhyTableNameEnabled);

        /**
         * 分库分表数处理 group数目 没有设置的时候则自动处理， 如果设置成0则报错 tablePerGroup数目
         * 没有设置的时候是1，如果设置成0则报错
         */
        int group_count;
        if (dbpartitionOptions.getDbpartitions() == null) {
            if (dbpartitionBy == null) {
                /**
                 * 只分表不分库
                 */
                group_count = 1;
            } else {
                /**
                 * 正常的分库, 只是没写dbpartitions的情况
                 */
                group_count = 0; /* 后面会自动进行替换 */
            }
        } else {
            if (dbpartitionOptions.getDbpartitions() < 1) {
                throw new IllegalArgumentException("dbpartitions should > 0");
            }
            group_count = dbpartitionOptions.getDbpartitions(); // 分库数
        }

        int table_count_on_each_group;
        if (dbpartitionOptions.getStartWith() != null || dbpartitionOptions.getEndWith() != null) {
            table_count_on_each_group = 2;
            needCheckTable = false;
            // 对于noloop版本的spe time, 分表的创建由指定的范围决定, 在规则计算之前不确定每个分库要创建多少张分表
            // 每个分库的表数暂时写死为2,
            // 这个逻辑决定着tbpattern，如果为1的话，说明每个分库只创建一张表则默认为逻辑表，所以这里的值要比1大即可
        } else if (dbpartitionOptions.getTbpartitions() == null) {
            table_count_on_each_group = 1;
        } else {
            if (dbpartitionOptions.getTbpartitions() < 1) {
                throw new IllegalArgumentException("tbpartitions should > 0");
            }
            table_count_on_each_group = dbpartitionOptions.getTbpartitions(); // 每个库的分表数
        }

        if (dbpartitionBy != null && dbpartitionBy.getColExpr().size() == 0) {
            throw new IllegalArgumentException("Can't set dbpartition key to empty!");
        }

        if (tbpartitionBy != null && tbpartitionBy.getColExpr().size() == 0) {
            throw new IllegalArgumentException("Can't set tbpartition key to empty!");
        }

        if (dbpartitionBy != null && (dbpartitionBy.getType() == PartitionByType.MM
            || dbpartitionBy.getType() == PartitionByType.DD || dbpartitionBy.getType() == PartitionByType.WEEK
            || dbpartitionBy.getType() == PartitionByType.MMDD)) {
            throw new IllegalArgumentException("Not support dbpartition method date");
        }

        /* 获得所有真实group的列表 */
        List<Group> dbList = optimizerContext.getMatrix().getGroups();

        /* 用于检查 */
        List<String> selGroupList = new ArrayList<String>();

        /* 计算缺省的dbname pattern */
        if (dbpartitionDefinition == null || dbpartitionDefinition.getPartition_name() == null) {
            /* 没有指定partition的形式的时候，自动生成 */
            String defaultDb = optimizerContext.getRuleManager().getDefaultDbIndex(null);

            String dbNamePattern =
                RuleUtils.genDBPatitionDefinition(group_count, table_count_on_each_group, dbList, defaultDb,
                    selGroupList);

            if (group_count == 0) {
                /* 根据自动推断取得最大相似的group大小 */
                group_count = selGroupList.size();
                if (group_count == 0) {
                    /* 如果group数没有指定,则设为默认8 */
                    group_count = 8;
                }
            }

            dbpartitionDefinition = new DBPartitionDefinition();
            dbpartitionDefinition.setPartition_name(SqlLiteral.createCharString(dbNamePattern, SqlParserPos.ZERO));
            dbpartitionDefinition.setStartWith(dbpartitionOptions.getStartWith());
            dbpartitionDefinition.setEndWith(dbpartitionOptions.getEndWith());

            tbpartitionDefinition = new TBPartitionDefinition();
            tbpartitionDefinition.setStartWith(dbpartitionOptions.getStartWith());
            tbpartitionDefinition.setEndWith(dbpartitionOptions.getEndWith());
        } else {
            /**
             * 如果已经设置了partition占位符，就需要根据占位符挑出合适的组，因为后面会对组进行合法性检测,
             * 这里直接将所有的物理group加到选择的列表中，只要选择的group能包含所有的推演出的group就代表OK
             */
            selGroupList.addAll(RuleUtils.groupToStringList(dbList));
        }

        /* 处理分库，分库dbNamePattern一定不为null */
        /* 但可以不是分库 */
        IPartitionGen partitionGen = TableRuleGenFactory.getInstance()
            .createDBPartitionGenerator(optimizerContext.getSchemaName(),
                dbpartitionBy == null ? PartitionByType.HASH : dbpartitionBy.getType());

        /**
         * <pre>
         * 分库分表键相同，并且方法也相同，使用全局唯一的分表表名
         *
         * 如果分库分表键不同，包括分表用时间都使用单独的分库和分表描述方式，分表在每个分库中唯一，分表表名跨库重复
         * </pre>
         */
        PartitionByType tbPartitionType;
        List<String> tbPartitionColExpr;
        if (tbpartitionBy == null) {

            /**
             * DDL中没有指定分表键(即tbPartition)，则是分库不分表，
             *
             * <pre>
             * 这时默认分库分表键相同, 使用useStandAlone=false模式;
             * ， 因为如果这种情况下，默认分库分表键不相同，那么库表就会独立枚举，这里每个库的表名就会变成
             *
             *  xxx_tbl_0, 而不是xxx_tbl.
             * </pre>
             */
            tbPartitionType = dbpartitionBy.getType();
            tbPartitionColExpr = dbpartitionBy.getColExpr();
        } else {
            tbPartitionType = tbpartitionBy.getType();
            tbPartitionColExpr = tbpartitionBy.getColExpr();
        }

        ISubpartitionGen subpartitionGen = TableRuleGenFactory.getInstance()
            .createTBPartitionGenerator(optimizerContext.getSchemaName(),
                dbpartitionBy == null ? PartitionByType.HASH : dbpartitionBy.getType(), tbPartitionType);

        /**
         * 默认使用全局唯一的方式生成规则
         */
        boolean useStandAlone = false;
        List<String> dbPartitionParams = new ArrayList<String>();
        List<String> tbPartitionParams = new ArrayList<String>();
        if (dbpartitionBy != null) {
            dbPartitionParams = dbpartitionBy.getColExpr();
        }
        if (tbpartitionBy != null) {
            tbPartitionParams = tbpartitionBy.getColExpr();
        }

        if (!comparePartitionParamList(dbPartitionParams, tbPartitionParams)) {
            /* 分库分表键不同 */
            useStandAlone = true;
        } else if (dbPartitionParams.isEmpty() || tbPartitionParams.isEmpty()) {
            useStandAlone = true;
        } else {

            /* 分库分表键相同 */
            if (dbpartitionBy.getType() != tbPartitionType) {
                /* 分表键相同，但方式不同，也需要借用分库分表键不同的形式生成唯一的分表名 */
                useStandAlone = true;
            }
            /* 或者其中有空字符串时，直接用原始逻辑表名当物理表名 */
        }

        // For recovery of CREATE TABLE.
        populateExistingRandomSuffix(tableName, tableRule, optimizerContext, randomPhyTableNameEnabled);

        if (useStandAlone) {
            /**
             * 使用独立方式生成分库分表规则
             */

            partitionGen.fillDbRuleStandAlone(tableRule, dbPartitionParams, tableMeta, group_count,
                table_count_on_each_group, dbpartitionDefinition);

            subpartitionGen.fillTbRuleStandAlone(tableRule, tbPartitionParams, tableMeta, group_count,
                table_count_on_each_group, tableName, tbpartitionDefinition);
        } else {
            /**
             * 使用全局唯一方式生成分库分表规则
             */

            partitionGen.fillDbRule(tableRule, dbPartitionParams, // name
                tableMeta, group_count, // 分库数
                table_count_on_each_group, // 每库分表数
                dbpartitionDefinition /* dbNamePattern */);

            subpartitionGen.fillTbRule(tableRule, tbPartitionParams, // name
                tableMeta, group_count, // 分库数
                table_count_on_each_group, // 每个库的物理表数
                tableName, tbpartitionDefinition /* tbNamePattern */);
        }

        if (dbpartitionBy != null && tbpartitionBy != null && dbpartitionBy.getType() != null
            && tbpartitionBy.getType() != null) {
            if (dbpartitionBy.getType().canCoverRule() && tbpartitionBy.getType().canCoverRule()) {
                tableRule.setCoverRule(true);
            }
        }

        /* 总是加上allowfulltablescan */
        tableRule.setAllowFullTableScan(true);

        /**
         * 这里可以直接做规则推导,因为来源是从SQL来的不存在 并发竞争的问题.
         */
        tableRule.init();

        ShardFuncParamsChecker.validateShardFuncionForTableRule(tableRule);

        /**
         * 这里特别的有一种情况，因为前面是通过真实的group列表来生成tableRule的
         * 但这里是通过这个抽象的tableRule来枚举出计算出的group列表的，这就要求
         * 真实的group必须是连续的，否则如果真实的是0,1,3,4，我这里计算的结果则会是
         * 0,1,2,3就会出错，而且这种错误是在执行的时候才会发现的，而且即使我在这里可以将
         * 真实的groupList带过来，但是执行的时候还是会shard到不存在的group中，这样还不如在
         * 建库的时候直接报错比较好，所以此处需要做预先校验工作
         */
        Map<String, Set<String>> topology = tableRule.getActualTopology();

        /* 只要选择的group能包含所有的推演出的group就代表OK */
        if (!RuleUtils.checkIfGroupMatch(new HashSet<String>(selGroupList), topology.keySet())) {
            throw new IllegalArgumentException("Selected physical group list is invalid:" + selGroupList);
        }

        /**
         * 因为前面的各种方式目的就是保证所有分表编号全局唯一，所以 这里进行最后的检查
         */
        String dbRule = (tableRule.getDbRuleStrs() == null || tableRule.getDbRuleStrs().length == 0) ? null :
            tableRule.getDbRuleStrs()[0];
        String tbRule = (tableRule.getTbRulesStrs() == null || tableRule.getTbRulesStrs().length == 0) ? null :
            tableRule.getTbRulesStrs()[0];

        /**
         * 检查最终生成的分表数与期望分表数是否相同
         */
        if (!RuleUtils.checkIfTableNumberOk(topology, group_count, table_count_on_each_group) && needCheckTable) {

            int actualTbSize = topology.values().iterator().next().size();
            String errorMsg = String.format(
                "The params of ddl may be invalid, because the expected physical tables number for each group specified by ddl mismatch the actual generated number, the expected/actual number is [%s/%s]",
                table_count_on_each_group, actualTbSize);
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errorMsg);

        }

        return tableRule;
    }

    public static void populateExistingRandomSuffix(String tableName, TableRule tableRule,
                                                    OptimizerContext optimizerContext,
                                                    boolean randomPhyTableNameEnabled) {
        try {
            TddlRule tddlRule = optimizerContext.getRuleManager().getTddlRule();
            if (tddlRule != null && randomPhyTableNameEnabled) {
                TableRule existingTableRule = optimizerContext.getRuleManager().getTableRule(tableName);
                if (existingTableRule != null) {
                    String randomSuffix = existingTableRule.getExistingRandomSuffixForRecovery();
                    if (TStringUtil.isEmpty(randomSuffix)) {
                        randomSuffix = existingTableRule.extractRandomSuffix();
                    }
                    if (TStringUtil.isNotEmpty(randomSuffix)) {
                        tableRule.setExistingRandomSuffixForRecovery(randomSuffix);
                    }
                } else {
                    String tableNamePrefix = tddlRule.getTableNamePrefixForShadowTable(tableName);
                    if (TStringUtil.isNotEmpty(tableNamePrefix)) {
                        tableRule.setTableNamePrefixForShadowTable(tableNamePrefix);
                    }
                }
            }
        } catch (Throwable t) {
            // Ignored
        }
    }

    /**
     * 比较两个Partition的参数列表是否完全一致
     *
     * @return 返回true 表示完全一致；否则为不一致
     */
    private static boolean comparePartitionParamList(List<String> paramList1, List<String> paramList2) {

        if (paramList1 == null && paramList2 != null) {
            return false;
        }

        if (paramList1 != null && paramList2 == null) {
            return false;
        }

        if (paramList1 == null && paramList2 == null) {
            return true;
        }

        if (paramList1.size() != paramList2.size()) {
            return false;
        }

        int paramCount = paramList1.size();
        for (int i = 0; i < paramCount; i++) {
            if (!paramList1.get(i).equals(paramList2.get(i))) {
                return false;
            }
        }

        return true;

    }

    public TableRule getTableRule() {
        return tableRule;
    }

    /**
     *
     */
    private String genTargetActualTableName(String schemaName, String origLogicalTableName, String toLocalTableName,
                                            String origActualTableName) {
        return toLocalTableName + getTargetActualTableNameSuffix(origLogicalTableName, origActualTableName);
    }

    /**
     * get the phy table name after renaming
     */
    private String getTargetActualTableNameSuffix(String origLogicalTableName, String origActualTableName) {
        // We still need to rename physical tables for logical tables created
        // before logical rename table support, otherwise user may fail to
        // create a new table with the same logical name after renaming such
        // logical tables.
        if (origLogicalTableName != null && origActualTableName != null
            && origLogicalTableName.length() < origActualTableName.length()) {
            return origActualTableName.substring(origLogicalTableName.length());
        } else {
            return "";
        }
    }

    public SqlKind getKind() {
        return sqlNode.getKind();
    }

    public Map<String, List<List<String>>> getTargetTablesHintCache() {
        return targetTablesHintCache;
    }

    /**
     * For HINT use only!
     */
    public void setTargetTablesHintCache(Map<String, List<List<String>>> targetTablesHintCache) {
        this.targetTablesHintCache = targetTablesHintCache;
    }

    public String getDbIndex() {
        return dbIndex;
    }

    public void setDbIndex(String dbIndex) {
        this.dbIndex = dbIndex;
    }

    public List<String> getPhyTables() {
        return phyTables;
    }

    public void setPhyTables(List<String> phyTables) {
        this.phyTables = phyTables;
    }

    public Map<String, TableRule> getGsiTableRules() {
        return gsiTableRules;
    }

    public Map<String, SqlNode> getGsiParentSqlNode() {
        return gsiParentSqlNode;
    }

    public List<Set<AlterColumnSpecification>> getAlterColumnSpecificationSets() {
        return alterColumnSpecificationSets;
    }

    public SqlNode getTruncateCreate() {
        return truncateCreate;
    }

    public void setTruncateCreate(SqlNode truncateCreate) {
        this.truncateCreate = truncateCreate;
    }

    /**
     * truncate table with recyclebin
     */
    public String getBinName() {
        return binName;
    }

    public void setBinName(String binName) {
        this.binName = binName;
    }

    public String getTruncateTableName() {
        return truncateTableName;
    }

    public void setTruncateTableName(String truncateTableName) {
        this.truncateTableName = truncateTableName;
    }

    @Override
    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    private static boolean partitionByTypeAllowedInHotMapping(PartitionByType partitionByType) {
        if (partitionByType == null) {
            return true;
        }
        return partitionByType == PartitionByType.UNI_HASH || partitionByType == PartitionByType.HASH
            || partitionByType == PartitionByType.STR_HASH;
    }

    private boolean containsShardingColumn(String colName) {
        boolean res = false;
        if (tableRule != null) {
            for (String shardColumn : tableRule.getShardColumns()) {
                if (colName.equalsIgnoreCase(shardColumn)) {
                    res = true;
                    break;
                }
            }
        }
        return res;
    }

}
