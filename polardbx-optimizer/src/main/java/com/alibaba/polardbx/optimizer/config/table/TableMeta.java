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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.ColumnarOptions;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ColumnarConfig;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableIdVersionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.schema.MetaDbSchema;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupVersionManager;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.utils.SchemaVersionManager;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_ENUM;

/**
 * 一个table的描述，包含主键信息/字段信息/索引信息等，暂时不考虑外键/约束键，目前没意义
 *
 * @author whisper
 */
@SkipSerializeInDdlDumpInfo({
    "digest", "tableGroupDigest", "schemaDigest", "initializerExpressionFactory", "id", "localPartitionDefinitionInfo",
    "version", "ttlDefinitionInfo", "tableFilesMeta", "gsiTableMetaBean", "gsiPublished", "columnarIndexPublished",
    "columnarIndexChecking", "archiveColumnarIndexPublished",
    "hasLogicalGeneratedColumnCache", "hasDefaultExprColumnCache", "hasGeneratedColumnCache",
    "hasExternalizedColumnCache", "externalizedColumnNamesCache"})
public class TableMeta implements Serializable, Cloneable, Table, Wrapper {

    private static final long serialVersionUID = 5168519373619656091L;
    private String digest;
    /**
     * the table group version of table
     */
    private List<String> tableGroupDigest = null;

    /**
     * the schema version of table
     */
    private List<String> schemaDigest = null;

    // id in metadb
    private long id;

    private String schemaName = null;

    /**
     * 表名
     */
    private final String tableName;

    private final TableStatus status;

    private final long version;

    private Engine engine;

    private String externalCatalogName;

    private final long flag;

    /**
     * 主键索引描述
     */
    private final Map<String, IndexMeta> primaryIndexes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * 二级索引描述
     */
    private final Map<String, IndexMeta> secondaryIndexes =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private final ImmutableList<IndexMeta> allIndexes;

    /**
     * Foreign key.
     */
    private final Map<String, ForeignKeyData> foreignKeys = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    /**
     * Constraints.
     * <Constraint Type, Set<Constraint Name>>
     */
    private final Map<String, Set<String>> constraints = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    /**
     * Referenced foreign key.
     * <constrained schema/table/index name, ForeignKeyData>
     */
    private final Map<String, ForeignKeyData> referencedForeignKeys = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private final Map<String, ColumnMeta> primaryKeys =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    private final Map<String, ColumnMeta> columns =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    private final Map<String, ColumnMeta> allColumns =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    private final List<ColumnMeta> allColumnsOrderByDefined = new ArrayList<>();

    // cache
    private volatile List<ColumnMeta> readColumnsCache = null;
    private volatile List<ColumnMeta> writeColumnsCache = null;
    private Boolean hasLogicalGeneratedColumnCache = null;
    private Boolean hasDefaultExprColumnCache = null;
    private Boolean hasGeneratedColumnCache = null;
    private Boolean hasExternalizedColumnCache = null;
    private List<String> externalizedColumnNamesCache = null;

    private boolean hasPrimaryKey = true;

    private TableColumnMeta tableColumnMeta = null;

    private List<ColumnMeta> autoUpdateColumns = null;
    private GsiMetaManager.GsiTableMetaBean gsiTableMetaBean = null;
    private Map<String, GsiIndexMetaBean> gsiPublished = null;
    private Map<String, GsiIndexMetaBean> columnarIndexPublished = null;
    private Map<String, GsiIndexMetaBean> columnarIndexChecking = null;
    private Map<String, GsiIndexMetaBean> archiveColumnarIndexPublished = null;
    private Map<String, GsiIndexMetaBean> snapshotColumnarIndexPublished = null;
    private Map<String, GsiIndexMetaBean> columnarIndexIgnored = null;
    private Map<String, GsiIndexMetaBean> nonArchiveColumnarIndexPublished = null;

    private ComplexTaskOutlineRecord complexTaskOutlineRecord = null;
    private ComplexTaskMetaManager.ComplexTaskTableMetaBean complexTaskTableMetaBean = null;

    private final InitializerExpressionFactory initializerExpressionFactory =
        new TableMetaInitializerExpressionFactory();

    private volatile boolean isAutoPartition = false;

    /**
     * Per-column MCE control state loaded from the {@code mce_column_state} system table. Key is
     * lower-case content column name. Completed columns no longer have a control row and derive
     * EXTERNALIZED from the column flag.
     */
    private volatile Map<String, ColumnMceState> columnMceStateMap = null;
    private volatile Map<String, String> columnMceAddrColumnMap = null;

    private volatile PartitionInfo partitionInfo = null;

    // when split/merge/move the table, this entry will save the new partitionInfo temporarily
    private volatile PartitionInfo newPartitionInfo = null;

    // for oss engine
    private Map<String, Map<String, List<FileMeta>>> fileMetaSet = null;
    private Map<String, List<FileMeta>> flatFileMetas = null;

    /**
     * for columnar column mapping
     * <tableId, List<filedId>>
     */
    private Map<Long, List<Long>> columnarFieldIdList = new ConcurrentHashMap<>();

    /**
     * for columnar sort keys
     * <tableId, List<filedId>>
     */
    private Map<Long, List<OrderByOption>> columnarSortKeys = new ConcurrentHashMap<>();

    /**
     * {@code Map<Pair<String, String>, MultiVersionedId>}
     * Cache mapping from cci table name to table ids
     * table id 可能由于重建而存在多版本，根据创建时的tso以跳表进行存储
     * 避免大小写问题，schema_name/cci_name 统一使用小写
     */
    @Getter
    private final Map<Pair<String, String>, MultiVersionedId> tableMappingCache = new ConcurrentHashMap<>();

    private volatile LocalPartitionDefinitionInfo localPartitionDefinitionInfo;

    private volatile TtlDefinitionInfo ttlDefinitionInfo;

    private volatile TableFilesMeta tableFilesMeta = null;

    private String defaultCharset;

    private String defaultCollation;

    private boolean encryption;

    private String comment;

    /**
     * TODO: fill this meta info when creating ledger table
     */
    private boolean blockChainHistory = false;

    public TableMeta(String schemaName, String tableName, List<ColumnMeta> allColumnsOrderByDefined,
                     IndexMeta primaryIndex,
                     List<IndexMeta> secondaryIndexes, boolean hasPrimaryKey, TableStatus status, long version,
                     long flag) {
        this(schemaName, tableName, allColumnsOrderByDefined, primaryIndex, secondaryIndexes, hasPrimaryKey, status,
            version, flag, null);
    }

    public TableMeta(String schemaName, String tableName, List<ColumnMeta> allColumnsOrderByDefined,
                     IndexMeta primaryIndex,
                     List<IndexMeta> secondaryIndexes, boolean hasPrimaryKey, TableStatus status, long version,
                     long flag, String comment) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.hasPrimaryKey = hasPrimaryKey;
        ImmutableList.Builder<IndexMeta> indexBuilder = ImmutableList.builder();
        if (hasPrimaryKey && primaryIndex != null) {
            this.primaryIndexes.put(primaryIndex.getPhysicalIndexName(), primaryIndex);
            for (IndexColumnMeta meta : primaryIndex.getKeyColumnsExt()) {
                ColumnMeta c = meta.getColumnMeta();
                c.getField().setPrimary(true);
                this.primaryKeys.put(c.getName(), c);
            }
            indexBuilder.add(this.getPrimaryIndex());
        }

        if (secondaryIndexes != null) {
            for (IndexMeta one : secondaryIndexes) {
                this.secondaryIndexes.put(one.getPhysicalIndexName(), one);
            }
            indexBuilder.addAll(this.secondaryIndexes.values());
        }

        for (ColumnMeta column : allColumnsOrderByDefined) {
            this.allColumns.put(column.getName(), column);
        }
        this.allColumnsOrderByDefined.addAll(allColumnsOrderByDefined);
        this.status = status;
        this.version = version;
        this.flag = flag;
        this.digest = tableName + "#version:" + version;
        this.comment = comment;
        this.allIndexes = indexBuilder.build();
        prepareShare();
    }

    public void buildFileStoreMeta(Map<String, String> columnMapping, Map<String, ColumnMeta> columnMetaMap) {
        this.tableFilesMeta = new TableFilesMeta(columnMapping, columnMetaMap);
    }

    /**
     * Prepares the share information for the table by validating column and index metadata,
     * setting up key parts, and building key part information for each column.
     * <p>
     * This method performs the following operations:
     * 1. Validates that no column is prepared more than once.
     * 2. Iterates through all indexes to set up key part fields for non-special indexes.
     * 3. If a primary key exists:
     * - Marks columns in the primary key as part of all non-special keys and sort keys.
     * - Adds primary key parts to secondary keys.
     * 4. Builds key part information for each physical column.
     *
     * @throws TddlRuntimeException if a column is found to be prepared more than once.
     */
    public void prepareShare() {
        // Validate that no column is prepared more than once
        for (ColumnMeta columnMeta : getPhysicalColumns()) {
            if (columnMeta.isPartOfBuilt()) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "column " + columnMeta.getName() + " part built twice");
            }
        }

        List<IndexMeta> indexes = getIndexes();
        IndexMeta pk = null;
        int pkIndex = -1;

        // Process each index to set up key part fields
        for (int skIndex = 0; skIndex < indexes.size(); skIndex++) {
            IndexMeta indexMeta = indexes.get(skIndex);
            if (indexMeta.isPrimaryKeyIndex()) {
                pk = indexMeta;
                pkIndex = skIndex;
            }
            if (indexMeta.isSpecialIndex()) {
                continue;
            }
            for (int j = 0; j < indexMeta.getUserDefinedKeyParts(); j++) {
                IndexColumnMeta keyPart = indexMeta.getKeyColumnsExt().get(j);
                if (keyPart.hasColumn()) {
                    setUpKeyPartField(skIndex, keyPart);
                }
            }
        }

        // Handle primary key logic if it exists
        if (pk != null) {
            // Mark primary key columns as part of all non-special keys and sort keys
            for (int j = 0; j < pk.getUserDefinedKeyParts(); j++) {
                IndexColumnMeta keyPart = pk.getKeyColumnsExt().get(j);
                ColumnMeta field = keyPart.getColumnMeta();
                if (keyPart.getSubPart() == 0 &&
                    field.getField().getRelType().getSqlTypeName() != SqlTypeName.BLOB) {
                    for (int skIndex = 0; skIndex < indexes.size(); skIndex++) {
                        if (!indexes.get(skIndex).isSpecialIndex()) {
                            field.setPartOfKey(skIndex);
                        }
                    }
                }
                if (keyPart.getSubPart() == 0) {
                    for (int skIndex = 0; skIndex < indexes.size(); skIndex++) {
                        if (!indexes.get(skIndex).isSpecialIndex()) {
                            field.setPartOfSortKey(skIndex);
                        }
                    }
                }
            }

            // Add primary key parts to secondary keys
            for (int i = 0; i < indexes.size(); i++) {
                addPkPartsToSk(indexes.get(i), i, pk, pkIndex);
            }
        }

        // Build key part information for each physical column
        for (ColumnMeta columnMeta : getPhysicalColumns()) {
            columnMeta.buildKeyPartInfo();
        }
    }

    void setUpKeyPartField(int keyIndex, IndexColumnMeta keyPart) {
        boolean fullLengthKeyPart = keyPart.getSubPart() == 0;
        if (fullLengthKeyPart) {
            keyPart.getColumnMeta().setPartOfKey(keyIndex);
            keyPart.getColumnMeta().setPartOfSortKey(keyIndex);
        } else {
            keyPart.getColumnMeta().setPartOfPrefixKey(keyIndex);
        }
    }

    /**
     * Adds primary key parts to secondary key index metadata.
     * This method enhances a secondary index by incorporating primary key columns that are not already present,
     * up to the maximum allowed index length. It also handles cleanup of key part references for columns
     * that exceed the index length limit.
     *
     * @param sk The secondary key index metadata to be modified
     * @param skIndex The index identifier for the secondary key
     * @param pk The primary key index metadata to extract columns from
     * @param pkIndex The index identifier for the primary key
     */
    void addPkPartsToSk(IndexMeta sk, int skIndex, IndexMeta pk, int pkIndex) {
        // Early return for special cases where PK parts should not be added
        if (sk.isSpecialIndex() || sk.isPrimaryKeyIndex()) {
            return;
        }
        if (skIndex == pkIndex) {
            return;
        }

        int pkPart = 0;
        // Iterate through primary key parts and add them to secondary key if not already present
        for (; pkPart < pk.getUserDefinedKeyParts(); pkPart++) {
            IndexColumnMeta pkKeyPart = pk.getKeyColumnsExt().get(pkPart);
            // Stop adding parts if we've reached the maximum index length
            if (sk.getActualKeyParts() >= ForceIndexUtil.INDEX_MAX_LEN) {
                break;
            }

            // Check if the primary key field is already included in the secondary key
            boolean pkFieldIsInSk = false;
            for (int j = 0; j < sk.getUserDefinedKeyParts(); j++) {
                if (sk.getKeyColumnsExt().get(j).getColumnMeta() == pkKeyPart.getColumnMeta()
                    && sk.getKeyColumnsExt().get(j).getSubPart() == pkKeyPart.getSubPart()) {
                    pkFieldIsInSk = true;
                    break;
                }
            }

            // Add the primary key part to the secondary key if it's not already included
            if (!pkFieldIsInSk) {
                //TODO: support MAX_KEY_LENGTH
                if (sk.isUniqueIndex()) {
                    continue;
                }
                setUpKeyPartField(skIndex, pkKeyPart);
                sk.addActualKeyParts();
            }
        }

        // Clear key part references for remaining primary key parts that couldn't be added due to length limits
        for (; pkPart < pk.getUserDefinedKeyParts(); pkPart++) {
            ColumnMeta field = pk.getKeyColumnsExt().get(pkPart).getColumnMeta();
            field.clearPartOfKey(skIndex);
            field.clearPartOfSortKey(skIndex);
        }
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public String getExternalCatalogName() {
        return externalCatalogName;
    }

    public void setExternalCatalogName(String externalCatalogName) {
        this.externalCatalogName = externalCatalogName;
    }

    private Map<String, String> externalOptions;

    public Map<String, String> getExternalOptions() {
        return externalOptions;
    }

    public void setExternalOptions(Map<String, String> externalOptions) {
        this.externalOptions = externalOptions;
    }

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public TableStatus getStatus() {
        return this.status;
    }

    public long getVersion() {
        return version;
    }

    public long getFlag() {
        return flag;
    }

    public boolean requireLogicalColumnOrder() {
        return (flag & TablesRecord.FLAG_LOGICAL_COLUMN_ORDER) != 0L;
    }

    public boolean rebuildingTable() {
        return (flag & TablesRecord.FLAG_REBUILDING_TABLE) != 0L;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.digest = schemaName + "." + tableName + "#version:" + version;
        this.schemaName = schemaName;
    }

    public IndexMeta getPrimaryIndex() {
        if (!hasPrimaryKey) {
            return null;
        }
        return primaryIndexes.isEmpty() ? null : primaryIndexes.values().iterator().next();
    }

    public String getDefaultCharset() {
        return defaultCharset;
    }

    public void setDefaultCharset(String defaultCharset) {
        this.defaultCharset = defaultCharset;
    }

    private boolean indexContainsMultiWriteTargetColumn(IndexMeta indexMeta, ColumnMeta multiWriteTargetColumnMeta) {
        if (multiWriteTargetColumnMeta == null) {
            return false;
        }
        return indexMeta.getKeyColumns().stream()
            .anyMatch(cm -> cm.getName().equalsIgnoreCase(multiWriteTargetColumnMeta.getName()));
    }

    public List<IndexMeta> getSecondaryIndexes() {
        ColumnMeta multiWriteTargetColumnMeta = getColumnMultiWriteTargetColumnMeta();
        return secondaryIndexes.values().stream()
            .filter(im -> !indexContainsMultiWriteTargetColumn(im, multiWriteTargetColumnMeta))
            .collect(Collectors.toList());
    }

    public List<IndexMeta> getUniqueIndexes(boolean includingPrimaryIndex) {
        ArrayList<IndexMeta> uniqueIndexes = new ArrayList<>();

        if (hasPrimaryKey && includingPrimaryIndex) {
            uniqueIndexes.add(getPrimaryIndex());
        }

        ColumnMeta multiWriteTargetColumnMeta = getColumnMultiWriteTargetColumnMeta();
        for (IndexMeta indexMeta : getSecondaryIndexes()) {
            if (!indexMeta.isPrimaryKeyIndex() && indexMeta.isUniqueIndex() && !indexContainsMultiWriteTargetColumn(
                indexMeta, multiWriteTargetColumnMeta)) {
                uniqueIndexes.add(indexMeta);
            }
        }
        return uniqueIndexes;
    }

    public Map<String, IndexMeta> getSecondaryIndexesMap() {
        Map<String, IndexMeta> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ColumnMeta multiWriteTargetColumnMeta = getColumnMultiWriteTargetColumnMeta();
        for (Entry<String, IndexMeta> entry : secondaryIndexes.entrySet()) {
            if (!indexContainsMultiWriteTargetColumn(entry.getValue(), multiWriteTargetColumnMeta)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public Map<String, ForeignKeyData> getForeignKeys() {
        return foreignKeys;
    }

    public Map<String, ForeignKeyData> getReferencedForeignKeys() {
        return referencedForeignKeys;
    }

    public Map<String, Set<String>> getConstraints() {
        return constraints;
    }

    public Set<String> getConstraintsByType(String type) {
        return constraints.computeIfAbsent(type, k -> new HashSet<>());
    }

    public Collection<ColumnMeta> getPrimaryKey() {
        return primaryKeys.values();
    }

    public Collection<ColumnMeta> getGsiImplicitPrimaryKey() {
        final IndexMeta pk = secondaryIndexes.get(TddlConstants.UGSI_PK_INDEX_NAME);
        if (null == pk) {
            return new ArrayList<>();
        }
        return pk.getKeyColumns();
    }

    public Collection<ColumnMeta> getColumns() {
        return columns.values();
    }

    public Map<String, ColumnMeta> getPrimaryKeyMap() {
        return this.primaryKeys;
    }

    // Get all column ignore the status.
    public List<ColumnMeta> getPhysicalColumns() {
        return allColumnsOrderByDefined;
    }

    public List<ColumnMeta> getAllColumns() {   //兼容以前 可读的columns
        if (readColumnsCache == null) {
            synchronized (this) {
                if (readColumnsCache == null) {
                    readColumnsCache = allColumnsOrderByDefined.stream()
                        .filter(column -> column.getStatus() == ColumnStatus.PUBLIC
                            || column.getStatus() == ColumnStatus.MULTI_WRITE_SOURCE)
                        .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
                }
            }
        }
        return readColumnsCache;
    }

    public List<ColumnMeta> getWriteColumns() {  //可写的columns
        if (writeColumnsCache == null) {
            synchronized (this) {
                if (writeColumnsCache == null) {
                    writeColumnsCache = allColumnsOrderByDefined.stream()
                        .filter(column -> column.getStatus() == ColumnStatus.PUBLIC
                            || column.getStatus() == ColumnStatus.MULTI_WRITE_SOURCE
                            || column.getStatus() == ColumnStatus.WRITE_ONLY
                            || column.getStatus() == ColumnStatus.WRITE_REORG)
                        .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
                }
            }
        }
        return writeColumnsCache;
    }

    public ColumnMeta getColumnMultiWriteSourceColumnMeta() {
        for (ColumnMeta columnMeta : allColumnsOrderByDefined) {
            if (columnMeta.getStatus() == ColumnStatus.MULTI_WRITE_SOURCE) {
                return columnMeta;
            }
        }
        return null;
    }

    public ColumnMeta getColumnMultiWriteTargetColumnMeta() {
        for (ColumnMeta columnMeta : allColumnsOrderByDefined) {
            if (columnMeta.getStatus() == ColumnStatus.MULTI_WRITE_TARGET) {
                return columnMeta;
            }
        }
        return null;
    }

    public IndexMeta getIndexMeta(String indexName) {
        IndexMeta retMeta = primaryIndexes.get(indexName);
        if (retMeta != null) {
            return retMeta;
        }
        retMeta = secondaryIndexes.get(indexName);
        return retMeta;
    }

    public List<IndexMeta> getIndexes() {
        return allIndexes;
    }

    public boolean checkIndexNameExists(String indexName) {
        return findLocalIndexByName(indexName) != null || findGlobalSecondaryIndexByName(indexName) != null;
    }

    /**
     * Retrieves the local index metadata by its name.
     *
     * @param localIndexName the local index name
     * @return the IndexMeta object if found; otherwise, returns {@code null}
     */
    @Nullable
    public IndexMeta findLocalIndexByName(@NonNull String localIndexName) {
        // Normalize the input index name without trimming
        localIndexName = SQLUtils.normalizeNoTrim(localIndexName);

        // Handle primary key
        if (GeneralUtil.isPrimary(localIndexName)) {
            return getPrimaryIndex();
        }

        // Iterate through all indexes to find a non-primary key match
        for (IndexMeta indexMeta : getIndexes()) {
            if (!indexMeta.isPrimaryKeyIndex() && indexMeta.getPhysicalIndexName().equalsIgnoreCase(localIndexName)) {
                return indexMeta;
            }
        }

        // No matching index found
        return null;
    }

    /**
     * Retrieves the Global Secondary Index (GSI) metadata by its name.
     *
     * @param indexName the GSI name (case-insensitive)
     * @return the GsiIndexMetaBean object if found; otherwise, returns null
     */
    public GsiMetaManager.GsiIndexMetaBean findGlobalSecondaryIndexByName(@NonNull String indexName) {
        // Normalize the input index name
        String normalizedIndexName = SQLUtils.normalizeNoTrim(indexName);

        if (gsiPublished != null && !gsiPublished.isEmpty()) {
            for (GsiMetaManager.GsiIndexMetaBean gsiMeta : gsiPublished.values()) {
                if (gsiMeta.visibility != IndexVisibility.VISIBLE) {
                    continue;
                }
                String gsiIndexName = TddlSqlToRelConverter.unwrapGsiName(gsiMeta.indexName);
                if (gsiIndexName.equalsIgnoreCase(normalizedIndexName)) {
                    return gsiMeta;
                }
            }
        }

        if (columnarIndexPublished != null && !columnarIndexPublished.isEmpty()) {
            for (GsiMetaManager.GsiIndexMetaBean cciMeta : columnarIndexPublished.values()) {
                if (cciMeta.visibility != IndexVisibility.VISIBLE) {
                    continue;
                }
                String cciName = TddlSqlToRelConverter.unwrapGsiName(cciMeta.indexName);
                if (cciName.equalsIgnoreCase(normalizedIndexName)) {
                    return cciMeta;
                }
            }
        }

        return null;
    }

    public GsiMetaManager.GsiIndexMetaBean findGlobalSecondaryIndexByNameOrFullName(@NonNull String indexName) {
        // Normalize the input index name
        String normalizedIndexName = SQLUtils.normalizeNoTrim(indexName);

        if (gsiPublished != null && !gsiPublished.isEmpty()) {
            for (GsiMetaManager.GsiIndexMetaBean gsiMeta : gsiPublished.values()) {
                if (gsiMeta.visibility != IndexVisibility.VISIBLE) {
                    continue;
                }
                String gsiIndexName = TddlSqlToRelConverter.unwrapGsiName(gsiMeta.indexName);
                if (gsiMeta.indexName.equalsIgnoreCase(normalizedIndexName)
                    || gsiIndexName.equalsIgnoreCase(normalizedIndexName)) {
                    return gsiMeta;
                }
            }
        }

        if (columnarIndexPublished != null && !columnarIndexPublished.isEmpty()) {
            for (GsiMetaManager.GsiIndexMetaBean cciMeta : columnarIndexPublished.values()) {
                if (cciMeta.visibility != IndexVisibility.VISIBLE) {
                    continue;
                }
                String cciName = TddlSqlToRelConverter.unwrapGsiName(cciMeta.indexName);
                if (cciMeta.indexName.equalsIgnoreCase(normalizedIndexName)
                    || cciName.equalsIgnoreCase(normalizedIndexName)) {
                    return cciMeta;
                }
            }
        }

        return null;
    }

    public Set<String> getLocalIndexNames() {
        Set<String> indexes = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexes.addAll(
            this.getAllIndexes().stream().map(IndexMeta::getPhysicalIndexName).collect(Collectors.toList()));
        return indexes;
    }

    /**
     * use getIndexes for better performance.
     * Retrieves all indexes including primary and secondary indexes.
     *
     * @return a list of all IndexMeta objects, including the primary index (if exists) and all secondary indexes
     */
    public List<IndexMeta> getAllIndexes() {
        List<IndexMeta> indexes = new ArrayList<IndexMeta>();
        IndexMeta index = this.getPrimaryIndex();
        if (index != null) {
            indexes.add(this.getPrimaryIndex());
        }
        indexes.addAll(secondaryIndexes.values());
        return indexes;
    }

    public boolean isLastShardIndex(String indexName) {
        List<IndexMeta> allIndexes = getAllIndexes();
        IndexMeta targetIndexMeta = getIndexMeta(indexName);

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            List<String> partitionKeys = this.getPartitionInfo().getActualPartitionColumnsNotReorder();
            if (!targetIndexMeta.isCoverShardKey(partitionKeys)) {
                return false;
            }
            for (IndexMeta indexMeta : allIndexes) {
                if (indexMeta.isCoverShardKey(partitionKeys) &&
                    !StringUtils.equalsIgnoreCase(indexMeta.getPhysicalIndexName(), indexName)) {
                    // 存在其他 local index cover 了拆分键
                    return false;
                }
            }
            return true;
        } else {
            TableRule tableRule = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTddlRuleManager()
                .getTableRule(tableName);
            List<String> dbKeys = tableRule.getDbPartitionKeys();
            List<String> tbKeys = tableRule.getTbPartitionKeys();

            boolean coverDbKeys = targetIndexMeta.isCoverShardKey(dbKeys);
            boolean coverTbKeys = targetIndexMeta.isCoverShardKey(tbKeys);

            if (!coverDbKeys && !coverTbKeys) {
                return false;
            }

            for (IndexMeta indexMeta : allIndexes) {
                if (coverDbKeys && indexMeta.isCoverShardKey(dbKeys)
                    && !StringUtils.equalsIgnoreCase(indexMeta.getPhysicalIndexName(), indexName)) {
                    return false;
                }
                if (coverTbKeys && indexMeta.isCoverShardKey(tbKeys)
                    && !StringUtils.equalsIgnoreCase(indexMeta.getPhysicalIndexName(), indexName)) {
                    return false;
                }
            }
            return true;
        }
    }

    public ColumnMeta getColumn(String name) {
        if (allColumns.get(name) != null) {
            return allColumns.get(name);
        } else if (name.contains(".")) {
            return allColumns.get(name.split("\\.")[1]); // 避免转义
        }
        return allColumns.get(name);
    }

    public ColumnMeta getColumnIgnoreCase(String name) {
        // try to find columnMeta ignore dot first
        if (allColumns.containsKey(name)) {
            return allColumns.get(name);
        }
        // not find colName with dot , try split
        if (name.contains(".")) {
            name = name.split("\\.")[1]; // 避免转义
        }
        if (allColumns.containsKey(name)) {
            return allColumns.get(name);
        }
        return null;
    }

    /**
     * Find an externalized column by its physical mapping name (e.g. "content_addr_").
     * Only matches columns with the externalized flag set, so it won't collide with
     * OMC or other uses of mappingName.
     */
    public ColumnMeta getColumnByMappingName(String mappingName) {
        if (mappingName == null) {
            return null;
        }
        for (ColumnMeta cm : allColumnsOrderByDefined) {
            if (cm.isExternalizedColumn()
                && mappingName.equalsIgnoreCase(cm.getMappingName())) {
                return cm;
            }
        }
        return null;
    }

    /**
     * 判断列是否存在，建议DML中判断都用这个
     */
    public boolean containsColumn(String columnName) {
        return null != getColumnIgnoreCase(columnName) || (getTableColumnMeta() != null
            && getTableColumnMeta().isGsiModifying()
            && getTableColumnMeta().getColumnMultiWriteMapping().containsKey(columnName.toLowerCase()));
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public boolean isHasPrimaryKey() {
        return hasPrimaryKey;
    }

    public boolean hasGsiImplicitPrimaryKey() {
        return isGsi() && secondaryIndexes.containsKey(TddlConstants.UGSI_PK_INDEX_NAME);
    }

    public boolean hasForeignKey() {
        return null != getForeignKeys() && !getForeignKeys().isEmpty();
    }

    public boolean hasReferencedForeignKey() {
        return null != getReferencedForeignKeys() && !getReferencedForeignKeys().isEmpty();
    }

    public void setHasPrimaryKey(boolean hasPrimaryKey) {
        this.hasPrimaryKey = hasPrimaryKey;
    }

    public boolean isAutoPartition() {
        return isAutoPartition;
    }

    public void setAutoPartition(boolean autoPartition) {
        isAutoPartition = autoPartition;
    }

    public ColumnMeta getAutoIncrementColumn() {
        for (ColumnMeta column : getAllColumns()) {
            if (column.isAutoIncrement()) {
                return column;
            }
        }
        return null;
    }

    public double getRowCount(Context context) {
        if (MetaDbSchema.NAME.equalsIgnoreCase(schemaName)) {
            return 100;
        }
        if (ConfigDataMode.isFastMock()) {
            return 10;
        }
        PlannerContext pc = context == null ? null : context.unwrap(PlannerContext.class);
        boolean isNeedTrace = pc != null && pc.isNeedStatisticTrace();
        StatisticResult statisticResult =
            StatisticManager.getInstance().getRowCount(schemaName, tableName, isNeedTrace);
        if (isNeedTrace) {
            pc.recordStatisticTrace(statisticResult.getTrace());
        }
        long rowCount = statisticResult.getLongValue();
        return rowCount <= 0 ? 1 : rowCount;
    }

    public RelDataTypeField getRowTypeIgnoreCase(String colName, RelDataTypeFactory typeFactory) {
        for (int i = 0; i < allColumnsOrderByDefined.size(); i++) {
            ColumnMeta columnMeta = allColumnsOrderByDefined.get(i);
            if (colName.equalsIgnoreCase(columnMeta.getName())) {
                RelDataType relDataType = columnMeta.getField().getRelType();
                return new RelDataTypeFieldImpl(columnMeta.getName(), i, relDataType);
            }
        }
        if (colName.contains(".")) {
            colName = colName.split("\\.")[1]; // 避免转义
        }
        for (int i = 0; i < allColumnsOrderByDefined.size(); i++) {
            ColumnMeta columnMeta = allColumnsOrderByDefined.get(i);
            if (colName.equalsIgnoreCase(columnMeta.getName())) {
                RelDataType relDataType = columnMeta.getField().getRelType();
                return new RelDataTypeFieldImpl(columnMeta.getName(), i, relDataType);
            }
        }
        return null;
    }

    public RelDataType getPhysicalRowType(RelDataTypeFactory typeFactory) {
        return CalciteUtils.switchRowType(getPhysicalColumns(), typeFactory);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return CalciteUtils.switchRowType(getAllColumns(), typeFactory);
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(getRowCount(null), ImmutableList.<ImmutableBitSet>of());
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
                                                CalciteConnectionConfig config) {
        return false;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getLocalAutoIncrementColumns() {
        List<String> autoIncrementColumns = new ArrayList<>();
        for (Entry<String, ColumnMeta> entry : allColumns.entrySet()) {
            ColumnMeta meta = entry.getValue();
            if (meta.isLocalAutoIncrement()) {
                autoIncrementColumns.add(entry.getKey());
            }
        }
        return autoIncrementColumns;
    }

    public List<String> getAutoIncrementColumns() {
        List<String> autoIncrementColumns = new ArrayList<>();
        for (Entry<String, ColumnMeta> entry : allColumns.entrySet()) {
            ColumnMeta meta = entry.getValue();
            if (meta.isAutoIncrement()) {
                autoIncrementColumns.add(entry.getKey());
            }
        }
        return autoIncrementColumns;
    }

    public List<ColumnMeta> getAutoUpdateColumns() {
        if (autoUpdateColumns == null) {
            synchronized (this) {
                if (autoUpdateColumns == null) {
                    autoUpdateColumns = new ArrayList<>();
                    for (ColumnMeta column : allColumnsOrderByDefined) {
                        if (column.isAutoUpdateColumn()) {
                            autoUpdateColumns.add(column);
                        }
                    }
                }
            }
        }
        return autoUpdateColumns;
    }

    public boolean isAutoUpdateColumn(String columnName) {
        return Optional.ofNullable(getColumn(columnName))
            .map(ColumnMeta::isAutoUpdateColumn)
            .orElse(false);
    }

    public List<String> getLogicalGeneratedColumnNames() {
        List<String> generatedColumns = new ArrayList<>();
        // Get all generated columns regardless their status
        for (Entry<String, ColumnMeta> entry : allColumns.entrySet()) {
            ColumnMeta meta = entry.getValue();
            if (meta.isLogicalGeneratedColumn()) {
                generatedColumns.add(entry.getKey());
            }
        }
        return generatedColumns;
    }

    public List<String> getGeneratedColumnNames() {
        List<String> generatedColumns = new ArrayList<>();
        // Get all generated columns regardless their status
        for (Entry<String, ColumnMeta> entry : allColumns.entrySet()) {
            ColumnMeta meta = entry.getValue();
            if (meta.isGeneratedColumn()) {
                generatedColumns.add(entry.getKey());
            }
        }
        return generatedColumns;
    }

    public List<String> getPublicLogicalGeneratedColumnNames() {
        List<String> generatedColumns = new ArrayList<>();
        // Get all public generated columns
        for (ColumnMeta meta : getAllColumns()) {
            if (meta.isLogicalGeneratedColumn()) {
                generatedColumns.add(meta.getName());
            }
        }
        return generatedColumns;
    }

    public boolean hasLogicalGeneratedColumn() {
        if (hasLogicalGeneratedColumnCache == null) {
            hasLogicalGeneratedColumnCache =
                allColumns.values().stream().anyMatch(ColumnMeta::isLogicalGeneratedColumn);
        }
        return hasLogicalGeneratedColumnCache;
    }

    public boolean hasDefaultExprColumn() {
        if (hasDefaultExprColumnCache == null) {
            hasDefaultExprColumnCache = allColumns.values().stream().anyMatch(ColumnMeta::isDefaultExpr);
        }
        return hasDefaultExprColumnCache;
    }

    public boolean hasGeneratedColumn() {
        if (hasGeneratedColumnCache == null) {
            hasGeneratedColumnCache = allColumns.values().stream().anyMatch(ColumnMeta::isGeneratedColumn);
        }
        return hasGeneratedColumnCache;
    }

    public boolean hasExternalizedColumn() {
        if (hasExternalizedColumnCache == null) {
            hasExternalizedColumnCache = allColumns.values().stream().anyMatch(ColumnMeta::isExternalizedColumn);
        }
        return hasExternalizedColumnCache;
    }

    public List<String> getExternalizedColumnNames() {
        if (externalizedColumnNamesCache == null) {
            List<String> result = new ArrayList<>();
            for (Entry<String, ColumnMeta> entry : allColumns.entrySet()) {
                if (entry.getValue().isExternalizedColumn()) {
                    result.add(entry.getKey());
                }
            }
            // Immutable so callers can't mutate cached state.
            externalizedColumnNamesCache = java.util.Collections.unmodifiableList(result);
        }
        return externalizedColumnNamesCache;
    }

    public boolean hasUnpublishedLogicalGeneratedColumn() {
        for (Entry<String, ColumnMeta> entry : allColumns.entrySet()) {
            ColumnMeta meta = entry.getValue();
            if (meta.isLogicalGeneratedColumn() && (meta.getStatus() != ColumnStatus.PUBLIC
                || meta.getStatus() == ColumnStatus.MULTI_WRITE_SOURCE)) {
                return true;
            }
        }
        return false;
    }

    public TableColumnMeta getTableColumnMeta() {
        return tableColumnMeta;
    }

    public void setTableColumnMeta(TableColumnMeta tableColumnMeta) {
        this.tableColumnMeta = tableColumnMeta;
    }

    // ==================== MCE State ====================

    /**
     * Whether any column requires MCE dual-column write.
     */
    public boolean isMceDualWriteEnabled() {
        return hasColumnInMceMigration();
    }

    /**
     * Set the validated per-column MCE state map when loading TableMeta.
     */
    public void setColumnMceStateMap(Map<String, ColumnMceState> columnMceStateMap) {
        this.columnMceStateMap = columnMceStateMap;
    }

    public void setColumnMceAddrColumnMap(Map<String, String> columnMceAddrColumnMap) {
        this.columnMceAddrColumnMap = columnMceAddrColumnMap;
    }

    public String getMceAddrColumnName(String contentColumnName) {
        if (contentColumnName == null || columnMceAddrColumnMap == null) {
            return null;
        }
        return columnMceAddrColumnMap.get(contentColumnName.toLowerCase());
    }

    public String getMceContentColumnByAddr(String addrColumnName) {
        if (addrColumnName == null || columnMceAddrColumnMap == null) {
            return null;
        }
        for (Entry<String, String> entry : columnMceAddrColumnMap.entrySet()) {
            if (addrColumnName.equalsIgnoreCase(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    public boolean isMceAddrColumnName(String columnName) {
        return getMceContentColumnByAddr(columnName) != null;
    }

    /**
     * The authoritative per-column MCE state, judged by read/write paths.
     * <p>Layered resolution:
     * <ol>
     *   <li>If the column has a control or guard entry in {@link #columnMceStateMap}, return it.</li>
     *   <li>Otherwise derive the terminal state from the column flag: a column carrying
     *       FLAG_EXTERNALIZED_COLUMN is EXTERNALIZED; everything else is NONE.</li>
     * </ol>
     */
    public ColumnMceState getColumnMceState(String columnName) {
        if (columnName == null) {
            return ColumnMceState.NONE;
        }
        if (columnMceStateMap != null) {
            ColumnMceState transient0 = columnMceStateMap.get(columnName.toLowerCase());
            if (transient0 != null) {
                return transient0;
            }
        }
        ColumnMeta cm = getColumnIgnoreCase(columnName);
        if (cm != null && cm.isExternalizedColumn()) {
            return ColumnMceState.EXTERNALIZED;
        }
        return ColumnMceState.NONE;
    }

    /**
     * Whether any column of this table is currently mid-migration (DUAL_WRITE / READ_ADDR).
     */
    public boolean hasColumnInMceMigration() {
        return columnMceStateMap != null && columnMceStateMap.values().stream()
            .anyMatch(state -> state == ColumnMceState.DUAL_WRITE || state == ColumnMceState.READ_ADDR);
    }

    /**
     * Whether any column is inside the MCE lifecycle, including the initial state after the addr
     * column and control record have been created but before dual-write is enabled.
     */
    public boolean hasColumnInMceLifecycle() {
        return columnMceAddrColumnMap != null && !columnMceAddrColumnMap.isEmpty();
    }

    public GsiMetaManager.GsiTableMetaBean getGsiTableMetaBean() {
        return gsiTableMetaBean;
    }

    public Map<String, GsiIndexMetaBean> getGsiPublished() {
        return gsiPublished;
    }

    public Map<String, GsiIndexMetaBean> getColumnarIndexPublished() {
        return columnarIndexPublished;
    }

    public Map<String, GsiIndexMetaBean> getColumnarIndexChecking() {
        return columnarIndexChecking;
    }

    public void setGsiTableMetaBean(GsiMetaManager.GsiTableMetaBean gsiTableMetaBean) {
        this.gsiTableMetaBean = gsiTableMetaBean;
        if (null != gsiTableMetaBean && gsiTableMetaBean.tableType.isPrimary()) {
            this.gsiPublished = new HashMap<>();
            this.columnarIndexPublished = new HashMap<>();
            this.columnarIndexChecking = new HashMap<>();
            this.archiveColumnarIndexPublished = new HashMap<>();
            this.snapshotColumnarIndexPublished = new HashMap<>();
            this.nonArchiveColumnarIndexPublished = new HashMap<>();
            this.columnarIndexIgnored = new HashMap<>();
            for (Entry<String, GsiIndexMetaBean> indexMetaBeanEntry : gsiTableMetaBean.indexMap.entrySet()) {
                if (indexMetaBeanEntry.getValue().indexStatus.isWriteReorg()
                    && indexMetaBeanEntry.getValue().columnarIndex) {
                    // CCI is in checking state.
                    this.columnarIndexChecking.put(indexMetaBeanEntry.getKey(), indexMetaBeanEntry.getValue());
                }
                if (!indexMetaBeanEntry.getValue().indexStatus.isPublished()) {
                    continue;
                }
                if (indexMetaBeanEntry.getValue().columnarIndex) {
                    this.columnarIndexPublished.put(indexMetaBeanEntry.getKey(), indexMetaBeanEntry.getValue());
                    String columnarType = indexMetaBeanEntry.getValue().columnarOptions.get().get(ColumnarOptions.TYPE);
                    if (columnarType != null && columnarType.equalsIgnoreCase(ColumnarConfig.ARCHIVE)) {
                        this.archiveColumnarIndexPublished.put(indexMetaBeanEntry.getKey(),
                            indexMetaBeanEntry.getValue());
                    } else {
                        this.nonArchiveColumnarIndexPublished.put(indexMetaBeanEntry.getKey(),
                            indexMetaBeanEntry.getValue());
                    }
                    if (columnarType != null && columnarType.equalsIgnoreCase(ColumnarConfig.SNAPSHOT)) {
                        this.snapshotColumnarIndexPublished.put(indexMetaBeanEntry.getKey(),
                            indexMetaBeanEntry.getValue());
                    }
                    if (indexMetaBeanEntry.getValue().columnarOptions.get()
                        .containsKey(ColumnarOptions.COLUMNAR_IGNORE)) {
                        if (Boolean.parseBoolean(
                            indexMetaBeanEntry.getValue().columnarOptions.get().get(ColumnarOptions.COLUMNAR_IGNORE))) {
                            this.columnarIndexIgnored.put(indexMetaBeanEntry.getKey(), indexMetaBeanEntry.getValue());
                        }
                    }
                } else {
                    this.gsiPublished.put(indexMetaBeanEntry.getKey(), indexMetaBeanEntry.getValue());
                }
            }
        }
    }

    // CCI是GSI的子集，可能包含GSI和CCI
    public boolean withGsi() {
        return null != getGsiTableMetaBean() && getGsiTableMetaBean().tableType != GsiMetaManager.TableType.GSI
            && GeneralUtil.isNotEmpty(getGsiTableMetaBean().indexMap);
    }

    public boolean withGsi(String indexName) {
        return withGsi() && hasGsiIgnoreCase(indexName);
    }

    public boolean withCci() {
        return null != getGsiTableMetaBean() && getGsiTableMetaBean().tableType != GsiMetaManager.TableType.COLUMNAR
            && GeneralUtil.isNotEmpty(getGsiTableMetaBean().indexMap) && getGsiTableMetaBean().indexMap.values()
            .stream().anyMatch(index -> index.columnarIndex);
    }

    // CCI是GSI的子集，当indexMap中都为columnarIndex时，证明表中全部都是CCI
    public boolean allCci() {
        return withCci() && getGsiTableMetaBean().indexMap.values()
            .stream().allMatch(index -> index.columnarIndex);
    }

    public boolean withGsiExcludingPureCci() {
        return withGsi() && !allCci();
    }

    public boolean hasCci(String indexName) {
        Set<String> cciNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        getGsiTableMetaBean().indexMap.forEach((key, value) -> {
            if (value.columnarIndex) {
                cciNames.add(TddlSqlToRelConverter.unwrapGsiName(key));
            }
        });
        return cciNames.contains(TddlSqlToRelConverter.unwrapGsiName(indexName));
    }

    public boolean withCci(String indexName) {
        return withCci() && hasCci(indexName);
    }

    public Stream<String> gsiNameStream() {
        return withGsi() ? getGsiTableMetaBean().indexMap.keySet().stream() : Stream.empty();
    }

    public Stream<String> gsiNameStream(Predicate<GsiIndexMetaBean> filter) {
        return withGsi() ?
            getGsiTableMetaBean()
                .indexMap
                .values()
                .stream()
                .filter(filter)
                .map(imb -> imb.indexName)
            : Stream.empty();
    }

    public boolean hasGsiIgnoreCase(String indexName) {
        Set<String> gsiNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        getGsiTableMetaBean().indexMap.forEach((key, value) -> {
            gsiNames.add(TddlSqlToRelConverter.unwrapGsiName(key));
        });
        return gsiNames.contains(indexName);
    }

    public boolean withClustered() {
        return withGsi() && getGsiTableMetaBean().indexMap.values().stream().filter(bean -> !bean.columnarIndex)
            .anyMatch(bean -> bean.clusteredIndex);
    }

    public boolean withPublishedGsi() {
        return GeneralUtil.isNotEmpty(this.gsiPublished);
    }

    public boolean withPublishedGsi(String index) {
        return withPublishedGsi() && getGsiPublished().containsKey(index);
    }

    public boolean isGsi() {
        if (partitionInfo != null && partitionInfo.isGsi()) {
            return true;
        }
        return null != getGsiTableMetaBean() && getGsiTableMetaBean().tableType == GsiMetaManager.TableType.GSI;
    }

    public boolean isColumnar() {
        if (partitionInfo != null && partitionInfo.isColumnar()) {
            return true;
        }
        return null != getGsiTableMetaBean() && getGsiTableMetaBean().tableType == GsiMetaManager.TableType.COLUMNAR;
    }

    public boolean withColumnar() {
        return null != getGsiTableMetaBean() && getGsiTableMetaBean().tableType != GsiMetaManager.TableType.COLUMNAR
            && GeneralUtil.isNotEmpty(getGsiTableMetaBean().indexMap);
    }

    public boolean isIgnoreColumnar() {
        if (getColumnarIndexIgnored() == null) {
            return false;
        }

        return getColumnarIndexIgnored().containsKey(this.getTableName());
    }

    public boolean isColumnarSnapshot() {
        String ttlTable = this.columnarOriginTable();
        TableMeta ttlTm =
            OptimizerContext.getContext(this.getSchemaName()).getLatestSchemaManager().getTableWithNull(ttlTable);
        if (ttlTm == null) {
            return false;
        }
        if (ttlTm.getSnapshotColumnarIndexPublished() == null) {
            return false;
        }
        return ttlTm.getSnapshotColumnarIndexPublished().containsKey(this.getTableName());
    }

    public boolean isColumnarArchive() {
        String ttlTable = this.columnarOriginTable();
        TableMeta ttlTm =
            OptimizerContext.getContext(this.getSchemaName()).getLatestSchemaManager().getTableWithNull(ttlTable);
        if (ttlTm == null) {
            return false;
        }
        if (ttlTm.getArchiveColumnarIndexPublished() == null) {
            return false;
        }
        for (String key : ttlTm.getArchiveColumnarIndexPublished().keySet()) {
            if (key.equals(this.getTableName())) {
                return true;
            }
        }
        return false;
    }

    public String columnarOriginTable() {
        if (getGsiTableMetaBean() == null) {
            return null;
        }
        if (getGsiTableMetaBean().gsiMetaBean == null) {
            return null;
        }
        return getGsiTableMetaBean().gsiMetaBean.tableName.toLowerCase();
    }

    public boolean isClustered() {
        return isGsi() && getGsiTableMetaBean().gsiMetaBean.clusteredIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TableMeta) {
            return tableName.equals(((TableMeta) o).getTableName())
                && primaryIndexes.size() == ((TableMeta) o).primaryIndexes.size()
                && secondaryIndexes.size() == ((TableMeta) o).secondaryIndexes.size()
                && primaryKeys.size() == ((TableMeta) o).primaryKeys.size()
                && columns.size() == ((TableMeta) o).columns.size() && allColumns.size() == ((TableMeta) o).allColumns
                .size()
                && allColumnsOrderByDefined.size() == ((TableMeta) o).allColumnsOrderByDefined.size()
                && hasPrimaryKey == ((TableMeta) o).hasPrimaryKey
                && autoUpdateColumns == ((TableMeta) o).autoUpdateColumns
                && gsiTableMetaBean == ((TableMeta) o).gsiTableMetaBean
                && status == ((TableMeta) o).status
                && version == ((TableMeta) o).version
                && isAutoPartition == ((TableMeta) o).isAutoPartition;
        }
        return false;
    }

    public ComplexTaskOutlineRecord getComplexTaskOutlineRecord() {
        return complexTaskOutlineRecord;
    }

    public void setComplexTaskOutlineRecord(ComplexTaskOutlineRecord complexTaskOutlineRecord) {
        this.complexTaskOutlineRecord = complexTaskOutlineRecord;
    }

    public ComplexTaskMetaManager.ComplexTaskTableMetaBean getComplexTaskTableMetaBean() {
        return complexTaskTableMetaBean;
    }

    public void setComplexTaskTableMetaBean(
        ComplexTaskMetaManager.ComplexTaskTableMetaBean complexTaskTableMetaBean) {
        this.complexTaskTableMetaBean = complexTaskTableMetaBean;
    }

    public PartitionInfo getNewPartitionInfo() {
        return newPartitionInfo;
    }

    public void setNewPartitionInfo(PartitionInfo newPartitionInfo) {
        this.newPartitionInfo = newPartitionInfo;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public LocalPartitionDefinitionInfo getLocalPartitionDefinitionInfo() {
        return this.localPartitionDefinitionInfo;
    }

    public void setLocalPartitionDefinitionInfo(final LocalPartitionDefinitionInfo localPartitionDefinitionInfo) {
        this.localPartitionDefinitionInfo = localPartitionDefinitionInfo;
    }

    public String getDigest() {
        return this.digest;
    }

    public String getTableGroupDigest(Long trxId) {
        return this.tableGroupDigest.get((int) (trxId % TableGroupVersionManager.segmentLockSize));
    }

    public String getSchemaDigest(Long trxId) {
        return this.schemaDigest.get((int) (trxId % SchemaVersionManager.segmentLockSize));
    }

    public List<String> getTableGroupDigestList() {
        return this.tableGroupDigest;
    }

    public void setTableGroupDigestList(List<String> tableGroupDigest) {
        this.tableGroupDigest = tableGroupDigest;
    }

    public List<String> getSchemaDigestList() {
        return this.schemaDigest;
    }

    public void setSchemaDigestList(List<String> schemaDigest) {
        this.schemaDigest = schemaDigest;
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(initializerExpressionFactory)) {
            return aClass.cast(initializerExpressionFactory);
        } else if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        return null;
    }

    public void setDefaultCollation(String defaultCollation) {
        this.defaultCollation = defaultCollation;
    }

    public String getDefaultCollation() {
        return defaultCollation;
    }

    public TtlDefinitionInfo getTtlDefinitionInfo() {
        return ttlDefinitionInfo;
    }

    public void setTtlDefinitionInfo(TtlDefinitionInfo ttlDefinitionInfo) {
        this.ttlDefinitionInfo = ttlDefinitionInfo;
    }

    public Map<String, GsiIndexMetaBean> getArchiveColumnarIndexPublished() {
        return archiveColumnarIndexPublished;
    }

    public Map<String, GsiIndexMetaBean> getNonArchiveColumnarIndexPublished() {
        return nonArchiveColumnarIndexPublished;
    }

    public Map<String, GsiIndexMetaBean> getSnapshotColumnarIndexPublished() {
        return snapshotColumnarIndexPublished;
    }

    public Map<String, GsiIndexMetaBean> getColumnarIndexIgnored() {
        return columnarIndexIgnored;
    }

    private class TableMetaInitializerExpressionFactory extends NullInitializerExpressionFactory {
        @Override
        public RexNode newColumnDefaultValue(RelOptTable table, int iColumn, InitializerContext context) {
            final RelDataTypeField relDataTypeField = table.getRowType().getFieldList().get(iColumn);
            final String columnName = relDataTypeField.getName();
            final Field field = getColumn(columnName).getField();

            final DataType columnDataType = field.getDataType();
            final String columnDefaultStr = field.getDefault();
            final String columnExtraStr = field.getExtra();

            final RexBuilder rexBuilder = context.getRexBuilder();

            if (null == columnDefaultStr) {
                if (!field.isNullable()) {
                    // Column has no default value
                    return null;
                } else {
                    // Default value is NULL
                    return rexBuilder.makeLiteral(columnDefaultStr, relDataTypeField.getType(), false);
                }
            }

            if (InstanceVersion.isMYSQL80()) {
                ColumnMeta columnMeta = getColumnIgnoreCase(columnName);
                String expr = columnMeta.getField().getUnescapeDefault();
                if (columnMeta.isDefaultExpr()) {
                    SQLExpr sqlExpr =
                        new MySqlExprParser(com.alibaba.polardbx.druid.sql.parser.ByteString.from(expr)).expr();
                    if (!SQLExprUtils.isLiteralExpr(sqlExpr)) {
                        return getDefaultExpressionRex(sqlExpr, table);
                    }
                }

            }

            if (TStringUtil.containsIgnoreCase(columnDefaultStr, "CURRENT_TIMESTAMP")) {
                final int scale = field.getDataType().getScale();
                return rexBuilder.makeCall(TddlOperatorTable.CURRENT_TIMESTAMP, rexBuilder.makeIntLiteral(scale));
            }

            if (DataTypeUtil.isNumberSqlType(columnDataType)) {
                final Object converted = columnDataType.convertFrom(columnDefaultStr);
                return rexBuilder.makeLiteral(converted, relDataTypeField.getType(), false);
            }

            if (DataTypeUtil.isStringType(columnDataType)) {
                return rexBuilder.makeLiteral(columnDefaultStr, relDataTypeField.getType(), false);
            }

            if (DataTypeUtil.isDateType(columnDataType) || DataTypeUtil
                .equalsSemantically(columnDataType, DataTypes.BinaryType)) {
                final NlsString valueStr = new NlsString(columnDefaultStr, null, null);
                return rexBuilder.makeCharLiteral(valueStr);
            }

            if (columnDataType.fieldType() == MYSQL_TYPE_ENUM) {
                final NlsString valueStr = new NlsString(columnDefaultStr, null, null);
                return rexBuilder.makeCharLiteral(valueStr);
            }

            // Return null for unsupported data type
            return null;
        }

        @Override
        public RexNode newImplicitDefaultValue(RelOptTable table, int iColumn, InitializerContext context) {
            final RelDataTypeField relDataTypeField = table.getRowType().getFieldList().get(iColumn);
            final String columnName = relDataTypeField.getName();
            final Field field = getColumn(columnName).getField();
            final DataType columnDataType = field.getDataType();

            final RexBuilder rexBuilder = context.getRexBuilder();

            if (DataTypeUtil.isNumberSqlType(columnDataType)) {
                final Object converted = columnDataType.convertFrom("0");
                return rexBuilder.makeLiteral(converted, relDataTypeField.getType(), false);
            }

            if (DataTypeUtil.isStringType(columnDataType)) {
                return rexBuilder.makeLiteral("", relDataTypeField.getType(), false);
            }

            if (DataTypeUtil.equalsSemantically(columnDataType, DataTypes.TimestampType)) {
                final int scale = field.getDataType().getScale();
                return rexBuilder.makeCall(TddlOperatorTable.CURRENT_TIMESTAMP, rexBuilder.makeIntLiteral(scale));
            }

            return super.newImplicitDefaultValue(table, iColumn, context);
        }

        public RexNode getDefaultExpressionRex(SQLExpr sqlExpr, RelOptTable table) {
            ExecutionContext ec = new ExecutionContext();
            ec.setSchemaName(schemaName);

            FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(null, null);
            sqlExpr.accept(visitor);
            SqlCall sqlCall =
                new SqlBasicCall(SqlStdOperatorTable.GEN_COL_WRAPPER_FUNC, new SqlNode[] {visitor.getSqlNode()},
                    SqlParserPos.ZERO);
            SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, ec);

            RelOptCluster cluster = sqlConverter.createRelOptCluster();
            PlannerContext plannerContext = PlannerContext.getPlannerContext(cluster);

            return sqlConverter.getRexForDefaultExpr(table.getRowType(), sqlCall, plannerContext);
        }
    }

    public void initPartitionInfo(String schemaName, String tableName, TddlRuleManager rule) {
        initPartitionInfo(null, schemaName, tableName, rule);
    }

    public void initPartitionInfo(Connection conn, String schemaName, String tableName, TddlRuleManager rule) {
        rule.getPartitionInfoManager().reloadPartitionInfo(conn, schemaName, tableName);
        this.partitionInfo = rule.getPartitionInfoManager().getPartitionInfo(tableName);
    }

    /**
     * get the field id of a column for OSS table
     *
     * @param column column name
     * @return the same colum name if the table is an old file storage table
     */
    public String getColumnFieldId(String column) {
        return tableFilesMeta.columnMapping.get(column.toLowerCase());
    }

    @Nullable
    public List<Long> getColumnarFieldIdList(long tableId) {
        return columnarFieldIdList.get(tableId);
    }

    public void setColumnarFieldIdList(long tableId, List<Long> columnarFieldIdList) {
        this.columnarFieldIdList.put(tableId, columnarFieldIdList);
    }

    /**
     * get the field id of a column for CCI
     *
     * @param columnIndex column index
     * @return corresponding field id of the column
     */
    public long getColumnarFieldId(long tableId, int columnIndex) {
        return columnarFieldIdList.get(tableId).get(columnIndex);
    }

    @Nullable
    public List<OrderByOption> getColumnarSortKeys(long tableId) {
        return this.columnarSortKeys.get(tableId);
    }

    public void setColumnarSortKeys(long tableId, List<OrderByOption> columnarSortKeys) {
        this.columnarSortKeys.put(tableId, columnarSortKeys);
    }

    public void setFileMetaSet(Map<String, Map<String, List<FileMeta>>> fileMetaSet) {
        // only for file-store engine table
        Preconditions.checkArgument(Engine.isFileStore(this.getEngine()));
        Preconditions.checkArgument(tableFilesMeta != null, "File Storage Meta info is empty");
        tableFilesMeta.setFileMetaSet(fileMetaSet);
    }

    public Map<String, List<FileMeta>> getFlatFileMetas() {
        return tableFilesMeta.getFlatFileMetas();
    }

    public boolean isOldFileStorage() {
        if (tableFilesMeta == null) {
            return false;
        }
        return tableFilesMeta.isOldFileStorage();
    }

    public void initPartitionInfo(String schemaName, String tableName, TddlRuleManager rule,
                                  List<TablePartitionRecord> tablePartitionRecords,
                                  List<TablePartitionRecord> tablePartitionRecordsFromDelta) {
        rule.getPartitionInfoManager()
            .reloadPartitionInfo(schemaName, tableName, this, tablePartitionRecords, tablePartitionRecordsFromDelta);
        this.partitionInfo = rule.getPartitionInfoManager().getPartitionInfo(tableName);
    }

    public Map<String, Set<String>> getLatestTopology() {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPart) {
            return OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName)
                .getTopology();
        } else {
            return OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(tableName).getActualTopology();
        }
    }

    public boolean isEncryption() {
        return encryption;
    }

    public void setEncryption(boolean encryption) {
        this.encryption = encryption;
    }

    public boolean isBlockChainHistory() {
        // follows the name pattern
        return StringUtils.startsWith(tableName, "__")
            && StringUtils.endsWith(tableName, "_hist");
    }

    public boolean containFullTextIndex() {
        for (IndexMeta indexMeta : secondaryIndexes.values()) {
            if (indexMeta.isFullTextIndex()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 通过comment判断是否是区块链表
     */
    public boolean isPolardbxBlockChain() {
        return comment != null && comment.contains(TddlConstants.POLARDBX_BLOCK_CHAIN);
    }

    public static class MultiVersionedId {
        private final NavigableMap<Long, Long> versions = new ConcurrentSkipListMap<>();

        public void addVersion(Long tso, Long id) {
            versions.put(tso, id);
        }

        public void addVersion(Map<Long, Long> versions) {
            this.versions.putAll(versions);
        }

        public Long getId(Long tso) {
            Map.Entry<Long, Long> entry = versions.floorEntry(tso);
            return (entry != null) ? entry.getValue() : null;
        }

        public boolean containsTableId(Long tableId) {
            return versions.containsValue(tableId);
        }
    }

    public void loadTableMappingCache(TableInfoManager tableInfoManager, Pair<String, String> key) {
        String logicalSchema = key.getKey();
        String logicalTableName = key.getValue();

        List<ColumnarTableMappingRecord> records;
        records = tableInfoManager.queryColumnarTableMapping(logicalSchema, logicalTableName);

        if (records != null && !records.isEmpty()) {
            Map<Long, Long> tableIdsMap = new HashMap<>();
            ColumnarTableMappingRecord record = records.get(0);
            long tableId = record.tableId;

            // 已访问的tableId集合
            Set<Long> visitedTableIds = new HashSet<>();

            List<ColumnarTableIdVersionRecord> tableIdVersionRecords =
                tableInfoManager.queryByNewTableId(tableId);
            if (GeneralUtil.isNotEmpty(tableIdVersionRecords)) {
                while (GeneralUtil.isNotEmpty(tableIdVersionRecords)) {
                    ColumnarTableIdVersionRecord tableIdVersionRecord = tableIdVersionRecords.get(0);
                    // 检查是否存在循环引用
                    if (!visitedTableIds.add(tableIdVersionRecord.newTableId)) {
                        throw new RuntimeException("Detected cycle in tableIdVersion for tableId: "
                            + tableIdVersionRecord.newTableId);
                    }
                    tableIdsMap.put(tableIdVersionRecord.newVersionId, tableIdVersionRecord.newTableId);
                    tableIdsMap.put(tableIdVersionRecord.oldVersionId, tableIdVersionRecord.oldTableId);
                    tableId = tableIdVersionRecord.oldTableId;
                    tableIdVersionRecords = tableInfoManager.queryByNewTableId(tableId);
                }
            } else {
                ColumnarTableEvolutionRecord tableEvolutionRecord =
                    tableInfoManager.queryColumnarTableEvolutionFirst(tableId).get(0);
                tableIdsMap.put(tableEvolutionRecord.versionId, tableId);
            }

            MultiVersionedId versionedId = new MultiVersionedId();
            versionedId.addVersion(tableIdsMap);

            tableMappingCache.put(key, versionedId);
        }
    }

}
