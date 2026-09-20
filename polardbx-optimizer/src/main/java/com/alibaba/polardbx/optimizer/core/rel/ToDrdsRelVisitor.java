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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.MergedStorageInfo;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ExternalSchemaManager;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.core.datatype.BlobType;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.planner.rule.AccessPathRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemLeader;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemRefreshStorage;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemReloadStorage;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalBaseline;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalCcl;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalCheckTableRouting;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalRebalance;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalReplicateDatabase;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalRoutingRule;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalSet;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalWarmup;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalWarmupControl;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyDal;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterFileStorage;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterFunction;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterInstance;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterJoinGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterProcedure;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterRule;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterStoragePool;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterSystemSetConfig;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableArchivePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableCancelExpand;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableDropPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableExchangePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableExpandPartitions;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableExtractPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGhost;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupAddTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupDropPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupExtractPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMergePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupModifyPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMovePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupOptimizePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupRenamePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupReorgPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSetLocality;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSetPartitionsLocality;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSplitPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSplitPartitionByHotValue;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupTruncatePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableMergePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableModifyPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableMovePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableOptimizePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTablePartitionCount;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRemoveAutoPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRemovePartitioning;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRenamePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableReorgPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRepartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSetTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSplitPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSplitPartitionByHotValue;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableToggleFullScan;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableTruncatePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAnalyzeTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalChangeConsensusLeader;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCheckCci;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCheckGsi;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalClearFileStorage;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalConvertAllSequences;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateFileStorage;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateFunction;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateIndexInDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateJavaFunction;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateJoinGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateMaterializedView;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateProcedure;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateStoragePool;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateView;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropFileStorage;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropFunction;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndexInDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropJavaFunction;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropJoinGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropMaterializedView;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropProcedure;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropStoragePool;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropView;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalExternalCatalogDdl;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalGenericDdl;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalImportDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalImportSequence;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalInsertOverwrite;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalInspectIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalMergeTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalMoveDatabases;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalOptimizeTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalPushDownUdf;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRefreshTopology;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTables;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalSecretDdl;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalSequenceDdl;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalTruncateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalUnArchive;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil;
import com.alibaba.polardbx.optimizer.core.rel.util.DirectPlanCommonGroupInfo;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlMetaValidationUtil;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryType;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils.TableProperties;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.RecursiveCTE;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.rel.dal.Show;
import org.apache.calcite.rel.ddl.AlterDatabase;
import org.apache.calcite.rel.ddl.AlterExternalCatalog;
import org.apache.calcite.rel.ddl.AlterFileStorageAsOfTimestamp;
import org.apache.calcite.rel.ddl.AlterFileStorageBackup;
import org.apache.calcite.rel.ddl.AlterFileStoragePurgeBeforeTimestamp;
import org.apache.calcite.rel.ddl.AlterFunction;
import org.apache.calcite.rel.ddl.AlterInstance;
import org.apache.calcite.rel.ddl.AlterJoinGroup;
import org.apache.calcite.rel.ddl.AlterProcedure;
import org.apache.calcite.rel.ddl.AlterRule;
import org.apache.calcite.rel.ddl.AlterSecret;
import org.apache.calcite.rel.ddl.AlterStoragePool;
import org.apache.calcite.rel.ddl.AlterSystemSetConfig;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.ddl.AlterTableArchivePartition;
import org.apache.calcite.rel.ddl.AlterTableGhost;
import org.apache.calcite.rel.ddl.AlterTableGroupAddPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupAddTable;
import org.apache.calcite.rel.ddl.AlterTableGroupDropPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupExtractPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMergePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupModifyPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupOptimizePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupRenamePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupReorgPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSetLocality;
import org.apache.calcite.rel.ddl.AlterTableGroupSetPartitionsLocality;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartitionByHotValue;
import org.apache.calcite.rel.ddl.AlterTableGroupTruncatePartition;
import org.apache.calcite.rel.ddl.AlterTablePartitionCount;
import org.apache.calcite.rel.ddl.AlterTableRemoveAutoPartition;
import org.apache.calcite.rel.ddl.AlterTableRemovePartitioning;
import org.apache.calcite.rel.ddl.AlterTableRepartition;
import org.apache.calcite.rel.ddl.AlterTableSetTableGroup;
import org.apache.calcite.rel.ddl.AlterTableToggleFullScan;
import org.apache.calcite.rel.ddl.AnalyzeTable;
import org.apache.calcite.rel.ddl.ChangeConsensusRole;
import org.apache.calcite.rel.ddl.ClearFileStorage;
import org.apache.calcite.rel.ddl.ConvertAllSequences;
import org.apache.calcite.rel.ddl.CreateDatabase;
import org.apache.calcite.rel.ddl.CreateExternalCatalog;
import org.apache.calcite.rel.ddl.CreateFileStorage;
import org.apache.calcite.rel.ddl.CreateFunction;
import org.apache.calcite.rel.ddl.CreateIndex;
import org.apache.calcite.rel.ddl.CreateIndexInDatabase;
import org.apache.calcite.rel.ddl.CreateJavaFunction;
import org.apache.calcite.rel.ddl.CreateJoinGroup;
import org.apache.calcite.rel.ddl.CreateMaterializedView;
import org.apache.calcite.rel.ddl.CreateProcedure;
import org.apache.calcite.rel.ddl.CreateSecret;
import org.apache.calcite.rel.ddl.CreateStoragePool;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.rel.ddl.CreateTableGroup;
import org.apache.calcite.rel.ddl.CreateView;
import org.apache.calcite.rel.ddl.DropDatabase;
import org.apache.calcite.rel.ddl.DropExternalCatalog;
import org.apache.calcite.rel.ddl.DropFileStorage;
import org.apache.calcite.rel.ddl.DropFunction;
import org.apache.calcite.rel.ddl.DropIndex;
import org.apache.calcite.rel.ddl.DropIndexInDatabase;
import org.apache.calcite.rel.ddl.DropJavaFunction;
import org.apache.calcite.rel.ddl.DropJoinGroup;
import org.apache.calcite.rel.ddl.DropMaterializedView;
import org.apache.calcite.rel.ddl.DropProcedure;
import org.apache.calcite.rel.ddl.DropSecret;
import org.apache.calcite.rel.ddl.DropStoragePool;
import org.apache.calcite.rel.ddl.DropTable;
import org.apache.calcite.rel.ddl.DropTableGroup;
import org.apache.calcite.rel.ddl.DropView;
import org.apache.calcite.rel.ddl.GenericDdl;
import org.apache.calcite.rel.ddl.ImportDatabase;
import org.apache.calcite.rel.ddl.ImportSequence;
import org.apache.calcite.rel.ddl.InspectIndex;
import org.apache.calcite.rel.ddl.MergeTableGroup;
import org.apache.calcite.rel.ddl.MoveDatabase;
import org.apache.calcite.rel.ddl.OptimizeTable;
import org.apache.calcite.rel.ddl.PushDownUdf;
import org.apache.calcite.rel.ddl.RefreshTopology;
import org.apache.calcite.rel.ddl.RenameTable;
import org.apache.calcite.rel.ddl.RenameTables;
import org.apache.calcite.rel.ddl.SequenceDdl;
import org.apache.calcite.rel.ddl.TruncateTable;
import org.apache.calcite.rel.ddl.UnArchive;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalHybridUnion;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalRecyclebin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableCancelExpand;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableExchangePartition;
import org.apache.calcite.sql.SqlAlterTableExpandPartitions;
import org.apache.calcite.sql.SqlAlterTableExtractPartition;
import org.apache.calcite.sql.SqlAlterTableMergePartition;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableModifySubPartitionValues;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlAlterTableOptimizePartition;
import org.apache.calcite.sql.SqlAlterTableRemoveLocalPartition;
import org.apache.calcite.sql.SqlAlterTableRenamePartition;
import org.apache.calcite.sql.SqlAlterTableReorgPartition;
import org.apache.calcite.sql.SqlAlterTableRepartitionLocalPartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCheckColumnarIndex;
import org.apache.calcite.sql.SqlCheckGlobalIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOptimizeTable;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowLocalityInfo;
import org.apache.calcite.sql.SqlShowPhysicalDdl;
import org.apache.calcite.sql.SqlShowTables;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_DML_WITH_SUBQUERY;
import static com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow.DB_INDEX_MODE_RANDOM;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;

/**
 * 对RelNode进行转换,将其底层TableScan转换为 LogicalView
 *
 * @author lingce.ldm
 */
public class ToDrdsRelVisitor extends RelShuttleImpl {

    private static final Logger logger = LoggerFactory.getLogger(ToDrdsRelVisitor.class);

    protected static Set<String> systemDbName = new TreeSet<String>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    static {
        // information_schema
        // performance_schema
        // sys
        // mysql
        systemDbName.add("information_schema");
        systemDbName.add("performance_schema");
        systemDbName.add("sys");
        systemDbName.add("mysql");
    }

    // Whether all tables are broadcast
    private boolean allTableBroadcast = true;
    // all tables are single table
    private boolean allTableSingle = true;
    // Whether all tables are single and in the same group
    private boolean allTableSingleWithSameGroup = true;
    private Long allTableSingleTgId = null;
    // Whether all tables are single and in the same group and no broadcast
    // table
    private boolean allTableSingleNoBroadcast = true;
    // If all tables are single, which group are they?
    private String singleDbIndex = null;

    private LogicalView baseLogicalView = null;
    // table names from original plan
    private List<String> tableNames = new ArrayList<>();
    private List<Map<Long, String>> tableStorages = new ArrayList<>();
    private SqlKind sqlKind = SqlKind.SELECT;
    private LockMode lockMode = LockMode.UNDEF;
    private boolean shouldRemoveSchemaName = false;
    private boolean modifyBroadcastTable = false;
    private boolean modifyGsiTable = false;
    private List<String> schemaNames = Lists.newArrayList();
    private PlannerContext plannerContext = null;
    private List<TableProperties> modifiedTables = new ArrayList<>();
    private boolean withIndexHint = false;
    private boolean modifyShardingColumn = false;
    private boolean containUncertainValue = false;
    private boolean containComplexExpression = false;
    private boolean containScaleOutWritableTable = false;
    private boolean containReplicateWriableTable = false;
    private boolean containOnlineModifyColumnTable = false;
    private boolean containGeneratedColumn = false;
    private boolean modifyForeignKey = false;
    private boolean modifyExternalizedData = false;

    private SqlNode ast;
    private boolean existsWindow = false;
    private boolean existsNonPushDownFunc = false;
    private boolean existsIntersect = false;
    private boolean existsMinus = false;
    private boolean existsGroupingSets;
    private boolean modifyWithLimitOffset = false;
    private boolean existsOSSTable;
    private boolean existPagingForce = false;
    private boolean existForceColumnar = false;
    private boolean allTableHaveColumnar = true;

    private boolean existsCheckSum = false;
    private boolean existsUnpushableAgg = false;
    private boolean existsCheckSumV2 = false;

    private boolean outFileStatistics = false;

    private boolean existsUnPushedDynamicValues = false;

    /**
     * insert select 中select包含as of tso时，在RR隔离级别下，下推执行DN会将select变成当前读，快照失效了，这里禁止下推，RC隔离级别没事。
     */
    private boolean insertSelectWithFlashback = false;

    private boolean hasLocalForceIndex = false;

    /**
     * Label if broadcast table with locality exist
     */
    private boolean existsBroadcastTblWithLocality = false;

    private boolean mysql80;

    /**
     * The common groupInfo of all replicas/single/broadcast/one-phytbl-partitioned tables in plan
     * (only for auto-db )
     */
    private DirectPlanCommonGroupInfo commonGroupKeyInfo = new DirectPlanCommonGroupInfo();

    private Set<TableScan> ttlTableScanSet = new HashSet<>();

    private TtlQueryType ttlQueryType = null;

    public ToDrdsRelVisitor() {
    }

    public ToDrdsRelVisitor(SqlNode ast, PlannerContext plannerContext) {
        this.sqlKind = ast.getKind();
        this.lockMode = LockMode.getLockMode(ast);
        this.plannerContext = plannerContext;
        this.ast = ast;
        MergedStorageInfo mergedStorageInfo =
            plannerContext.getExecutionContext().getStorageInfo(plannerContext.getSchemaName());
        this.mysql80 = (mergedStorageInfo != null) && mergedStorageInfo.isMysql80();
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        this.existsGroupingSets = CBOUtil.isGroupSets(aggregate) || existsGroupingSets;
        this.existsCheckSum = CBOUtil.isCheckSum(aggregate) || this.existsCheckSum;
        this.existsUnpushableAgg = CBOUtil.containUnpushableAgg(aggregate) || this.existsUnpushableAgg;
        this.existsCheckSumV2 = CBOUtil.isCheckSumV2(aggregate) || this.existsCheckSumV2;
        return super.visit(aggregate);
    }

    @Override
    public RelNode visit(LogicalOutFile outFile) {
        this.outFileStatistics = outFile.getOutFileParams().getStatistics();
        return super.visit(outFile);
    }

    /**
     * 将 tableScan 替换为 LogicalView
     */
    @Override
    public final RelNode visit(TableScan scan) {
        final List<String> qualifiedName = scan.getTable().getQualifiedName();
        final String tableName = Util.last(scan.getTable().getQualifiedName());

        setShouldRemoveSchemaName(qualifiedName);

        final String schemaName = qualifiedName.size() == 2 ? qualifiedName.get(0) : null;
        final String schemaNameNotNull =
            schemaName == null ? this.plannerContext.getExecutionContext().getSchemaName() : schemaName;

        if (plannerContext.getExecutionContext().getParamManager()
            .getBoolean(ConnectionParams.ENABLE_TRANSPARENT_TTL)) {
            TableMeta tableMeta = plannerContext.getExecutionContext().getSchemaManager(schemaName).getTable(tableName);
            TtlDefinitionInfo ttlDefinitionInfo = tableMeta.getTtlDefinitionInfo();
            //必须是TTL表且是Query语句
            if (ttlDefinitionInfo != null && !ttlTableScanSet.contains(scan)
                && (sqlKind == SqlKind.SELECT || sqlKind == SqlKind.INSERT)) {
                RelNode node = buildTransparentTtlTableScan(scan, tableName, schemaNameNotNull);
                if (node != null) {
                    return node;
                }
            }
        }

        // Ensure that schema name not null.
        final RelNode scanOrLookup = buildTableAccess(scan, tableName,
            null == schemaName ? this.plannerContext.getExecutionContext().getSchemaName() : schemaName);

        final List<String> tableNames = ImmutableList.of(tableName);

        if (!schemaNames.contains(schemaName)) {
            schemaNames.add(schemaName);
        }
        final Map<String, TableProperties> tablePropertiesMap =
            RelUtils.buildTablePropertiesMap(tableNames, schemaName, this.plannerContext.getExecutionContext());

        updateTableProperties(tablePropertiesMap, scan);

        // FIXME: Will anything broken if baseLogicalView is null?
        if (baseLogicalView == null && scanOrLookup instanceof LogicalView) {
            baseLogicalView = (LogicalView) scanOrLookup;
        }
        this.tableNames.add(tableNames.get(0));
        Map<Long, String> tableStorage = null;
        if (tablePropertiesMap != null) {
            TableProperties tableProperties = tablePropertiesMap.get(tableNames.get(0));
            if (tableProperties != null) {
                tableStorage = tableProperties.getStorageIds();
            }
        }
        this.tableStorages.add(tableStorage);
        if (scanOrLookup instanceof LogicalView) {
            AccessPathRule.nomoralizeIndexNode((LogicalView) scanOrLookup);
        }
        if (scan.getFlashback() != null) {
            if (!RexUtil.isDeterministic(scan.getFlashback())) {
                containUncertainValue = true;
                existsNonPushDownFunc = true;
            }
        }
        if (!CBOUtil.hasCci(schemaNameNotNull, tableName, this.plannerContext.getExecutionContext())) {
            this.allTableHaveColumnar = false;
        }

        ExecutionContext ec = this.plannerContext.getExecutionContext();
        this.commonGroupKeyInfo.updateCommonGroupKeyByTableName(ec, schemaNameNotNull, tableName);
        if (this.commonGroupKeyInfo.isContainAnyReplicasTables()) {
            this.allTableBroadcast = false;
            this.allTableSingleNoBroadcast = false;
            this.allTableSingle = false;
            this.allTableHaveColumnar = false;
        }

        if (PlannerUtils.checkIfBroadcastTableWithLocality(schemaNameNotNull, tableName,
            this.plannerContext.getExecutionContext())) {
            this.existsBroadcastTblWithLocality = true;
        }
        return resolveExternalizedColumns(scanOrLookup, tableName, schemaNameNotNull);
    }

    /**
     * Add a FETCH_BLOB conversion Project above a row-store table access node (LogicalView or
     * LogicalTableLookup) if the table has externalized columns. OSSTableScan already exposes the
     * logical column value and must not be converted as a BlobRef.
     *
     * <p>This is the single, unified injection point for FETCH_BLOB in the entire planner.
     * It runs during ToDrdsRelVisitor (before optimize), so the FETCH_BLOB Project is visible
     * to all subsequent optimization rules. The Project is treated like a user-written function
     * call — as if the SQL originally had FETCH_BLOB(content) instead of bare content.
     *
     * <p>The table access below the Project still carries the physical BlobRef. TddlRelToSqlConverter
     * independently maps the logical field name to the addr column when producing DN SQL, while this
     * Project restores the logical TEXT/BLOB value for CN operators above it. DUAL_WRITE columns are not
     * marked externalized and keep reading plaintext; READ_ADDR and EXTERNALIZED columns are marked and
     * take this conversion. If no such column is present, the original access node is returned unchanged.
     *
     * <p>Lifecycle through optimization:
     * <ul>
     *   <li>Primary table: Project(FETCH_BLOB) stays above LogicalView; CBO may replace the
     *       LogicalView with IndexScan (covering) — the Project stays above unchanged.</li>
     *   <li>GSI table lookup: Project(FETCH_BLOB) above LogicalTableLookup. During RBO,
     *       ProjectTableLookupTransposeRule absorbs it into the internal Project. Then the
     *       user's SELECT Project prunes unused ext-cols via a second pass. If all needed
     *       columns are on the GSI side, TableLookupRemoveRule eliminates the lookup.</li>
     *   <li>Column pruning: when upper plan doesn't reference an ext-col, ProjectMergeRule
     *       or ProjectTableLookupTransposeRule drops the FETCH_BLOB expression naturally.</li>
     *   <li>DML (UPDATE/DELETE): PushModifyRule through-Project variants strip the FETCH_BLOB
     *       Project when pushing DML to DN.</li>
     *   <li>JOIN pushdown: PushJoinRule.containsCnOnlyFunction blocks join pushdown when
     *       FETCH_BLOB appears in the join condition (after JoinProjectTransposeRule inlines it).</li>
     * </ul>
     */
    private RelNode resolveExternalizedColumns(RelNode node, String tableName, String schemaName) {
        if (node instanceof OSSTableScan) {
            return node;
        }

        TableMeta tableMeta = plannerContext.getExecutionContext()
            .getSchemaManager(schemaName).getTableWithNull(tableName);
        if (tableMeta == null) {
            return node;
        }

        // For GSI (direct SELECT on the index table): resolve to the primary table.
        // The GSI index table physically holds blob addresses (content_addr_ reversed to
        // logical name 'content' by GMS). FETCH_BLOB still needs to be injected to restore
        // content from OSS, but the schema/table arguments must point to the primary table
        // because OSS object identity is keyed by primary table id.
        if (tableMeta.isGsi()) {
            String primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            String primarySchemaName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableSchema;
            if (primaryTableName == null || primarySchemaName == null) {
                return node;
            }
            try {
                TableMeta primaryMeta = OptimizerContext.getContext(primarySchemaName)
                    .getLatestSchemaManager().getTableWithNull(primaryTableName);
                if (primaryMeta == null) {
                    return node;
                }
                tableMeta = primaryMeta;
                tableName = primaryTableName;
                schemaName = primarySchemaName;
            } catch (Exception e) {
                return node;
            }
        }

        // The externalized flags that matter live on the (possibly GSI-resolved) primary meta:
        // ordinary tables bail out on the cached O(1) flag before any per-column work.
        if (!tableMeta.hasExternalizedColumn()) {
            return node;
        }

        Map<String, ColumnMeta> extColMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ColumnMeta cm : tableMeta.getAllColumns()) {
            if (cm.isExternalizedColumn()) {
                extColMap.put(cm.getName(), cm);
            }
        }
        if (extColMap.isEmpty()) {
            return node;
        }

        List<RelDataTypeField> fields = node.getRowType().getFieldList();
        RexBuilder rexBuilder = node.getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>(fields.size());
        List<String> fieldNames = new ArrayList<>(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            ColumnMeta extCol = extColMap.get(field.getName());
            if (extCol != null) {
                // Preserve the logical output name and declared type. Only the expression below changes from an
                // input BlobRef to restored content, so predicates/projects above this boundary keep normal SQL
                // column semantics and do not need to know the physical addr column name.
                boolean isBinary = extCol.getDataType() instanceof BlobType;
                projects.add(rexBuilder.makeCall(
                    TddlOperatorTable.FETCH_BLOB,
                    rexBuilder.makeInputRef(field.getType(), i),
                    rexBuilder.makeLiteral(schemaName.toLowerCase()),
                    rexBuilder.makeLiteral(tableName.toLowerCase()),
                    rexBuilder.makeLiteral(extCol.getName().toLowerCase()),
                    rexBuilder.makeLiteral(isBinary ? "BLOB" : "TEXT")));
            } else {
                projects.add(rexBuilder.makeInputRef(field.getType(), i));
            }
            fieldNames.add(field.getName());
        }
        return LogicalProject.create(node, projects, fieldNames);
    }

    private RelNode buildTableAccess(TableScan scan, String tableName, String schemaName) {
        assert schemaName != null;
        final RelOptSchema catalog = RelUtils.buildCatalogReader(Optional.ofNullable(schemaName)
                .orElseGet(() -> OptimizerContext.getContext(schemaName).getSchemaName()),
            plannerContext.getExecutionContext());

        if (scan.getIndexNode() instanceof SqlNodeList) {
            final Iterator<SqlNode> iterator = ((SqlNodeList) scan.getIndexNode()).iterator();
            while (iterator.hasNext()) {
                final SqlNode next = iterator.next();
                if (next instanceof SqlIndexHint) {
                    SqlIndexHint hint = (SqlIndexHint) next;
                    final String indexName =
                        GlobalIndexMeta.getIndexName(RelUtils.lastStringValue(hint.getIndexList()));
                    final String unwrapped = GlobalIndexMeta
                        .getGsiWrappedName(tableName, indexName, schemaName, plannerContext.getExecutionContext());
                    if (unwrapped != null) {
                        // Record the properties.
                        this.withIndexHint = true;
                    }
                }
            }
        }
        final TableMeta tMeta =
            this.plannerContext.getExecutionContext().getSchemaManager(schemaName).getTable(tableName);
        final Engine engine = tMeta.getEngine();

        // For external engine tables, construct ExternalTableScan directly.
        // Disable directPlan and postPlanner for external tables.
        // This runs before the force index paths below: an external table has neither GSI
        // nor local index, so those paths can only ever reach local-schema components that
        // an external OptimizerContext does not carry.
        if (engine == Engine.EXTERNAL) {
            if (getIndexHint(scan) != null || getForceIndex(scan) != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                    "Index hint is not supported on external table '" + tableName + "'");
            }
            this.existsOSSTable = true;
            Map<String, String> options = Maps.newHashMap();
            if (tMeta.getExternalOptions() != null) {
                options.putAll(tMeta.getExternalOptions());
            } else {
                String catalogName = tMeta.getExternalCatalogName();
                if (catalogName != null) {
                    ExternalCatalogInfo catInfo = ExternalCatalogManager.getInstance().get(catalogName);
                    if (catInfo != null) {
                        options.put("connector", catInfo.getConnector());
                        options.putAll(catInfo.getProperties());
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                            "External catalog '" + catalogName + "' not found or not loaded");
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                        "External table '" + tableName + "' has no catalog or options configured");
                }
            }
            final RelOptTable relOptTable = catalog.getTableForMember(ImmutableList.of(schemaName, tableName));
            TableSource tableSource = TableSource.create(options, relOptTable);
            tableSource.initPushDown(scan.getCluster(), schemaName);
            plannerContext.setHasExternalTableOperation(true);
            return ExternalTableScan.create(scan.getCluster(), relOptTable, tableSource);
        }

        // try index hint first
        hasLocalForceIndex = false;
        RelNode scanOrLookup = buildForceIndexByIndexHint(catalog, scan, schemaName, tMeta, engine);
        if (scanOrLookup != null) {
            if (hasLocalForceIndex) {
                // meaning index hint is working, we need to forbid the direct plan and post planner
                this.plannerContext.setLocalIndexHint(true);
            }
            return scanOrLookup;
        }

        // try force index
        scanOrLookup = buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine);
        if (scanOrLookup != null) {
            return scanOrLookup;
        }
        // For black hole, access cci table
        if (engine == Engine.BLACKHOLE) {
            scanOrLookup = buildBlackHoleAccessTable(catalog, scan, schemaName, tMeta);
            if (scanOrLookup != null) {
                return scanOrLookup;
            }
        }
        // remove all force index
        removeForceIndex(scan);
        if (CollectionUtils.isNotEmpty(IndexUtil.getPagingForceIndex(scan.getIndexNode()))) {
            this.existPagingForce = true;
        }
        return RelUtils.createLogicalView(scan, lockMode, engine);
    }

    private RelNode buildTransparentTtlTableScan(TableScan tableScan, String tableName, String schemaName) {
        TableMeta tableMeta = plannerContext.getExecutionContext().getSchemaManager(schemaName).getTable(tableName);
        TtlDefinitionInfo ttlDefinitionInfo = tableMeta.getTtlDefinitionInfo();
        if (ttlDefinitionInfo == null) {
            return null;
        }

        if (plannerContext.getExecutionContext().isInternalSystemSql()) {
            return null;
        }

        if (tableMeta.getArchiveColumnarIndexPublished() == null || tableMeta.getArchiveColumnarIndexPublished()
            .isEmpty()) {
            return null;
        }

        TtlQueryType hintTtlQueryType = null;
        try {
            hintTtlQueryType = TtlQueryType.valueOf(
                plannerContext.getExecutionContext().getParamManager().getString(ConnectionParams.TTL_QUERY_TYPE)
                    .toUpperCase());
        } catch (Throwable e) {
            //ignore
        }

        //对于非TtlHybrid的TTL表，可以使用hint生成行列执行计划。但是必须进行校验，符合TtlHybrid
        if (!Optional.ofNullable(ttlDefinitionInfo.getTtlInfoRecord().getExtra().getTtlHybrid()).orElse(false)) {
            if (hintTtlQueryType == null) {
                return null;
            } else {
                TtlMetaValidationUtil.validateTtlHybrid(ttlDefinitionInfo, tableMeta,
                    plannerContext.getExecutionContext(), true);
            }

        }

        //1.默认使用HOT_AND_COLD
        this.ttlQueryType = TtlQueryType.HOT_AND_COLD;
        //2.hint中的TtlQueryType具有更高优先级
        if (hintTtlQueryType != null) {
            this.ttlQueryType = hintTtlQueryType;
        }
        //3.ec中的TtlQueryType具有更高优先级，其只会在冷热分区裁剪之后会被设置，用于生成纯行存/列存执行计划
        if (plannerContext.getExecutionContext().getTtlQueryType() != null) {
            this.ttlQueryType = plannerContext.getExecutionContext().getTtlQueryType();
        }

        plannerContext.setTtlQueryType(this.ttlQueryType);
        if (this.ttlQueryType == TtlQueryType.HOT_COMMON) {
            return null;
        }

        RelOptSchema relOptSchema = RelUtils.buildCatalogReader(schemaName, plannerContext.getExecutionContext());
        RexBuilder rexBuilder = tableScan.getCluster().getRexBuilder();

        RexCall ttlQueryBoundaryCall =
            (RexCall) rexBuilder.makeCall(TddlOperatorTable.TTL_QUERY_BOUNDARY, rexBuilder.makeLiteral(schemaName),
                rexBuilder.makeLiteral(tableName));

        ColumnMeta columnMeta = ttlDefinitionInfo.getTtlColMeta(plannerContext.getExecutionContext());
        String ttlColName = columnMeta.getName();

        if (ttlDefinitionInfo.getTtlInfoRecord().getTtlFilter() != null
            && ttlDefinitionInfo.getTtlInfoRecord().getTtlFilter().length() > 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "ttl filter is not supported in ttl table query");
        }
        boolean ttlColNullable = columnMeta.isNullable();
        boolean ttlOnlyCleanUpNotNullRows = plannerContext.getExecutionContext().getParamManager()
            .getBoolean(ConnectionParams.TTL_ONLY_CLEANUP_NOT_NULL_ROWS);
        boolean ignoreTtlColNullable =
            plannerContext.getExecutionContext().getParamManager().getBoolean(ConnectionParams.IGNORE_TTL_COL_NULLABLE);

        //创建在线表的扫描计划
        RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(tableScan.getCluster(), relOptSchema);
        relBuilder.push(tableScan);
        RexInputRef ttlColInputRefForOnline = relBuilder.field(ttlColName);
        RexNode ttlFilterForOnline =
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ttlColInputRefForOnline,
                ttlQueryBoundaryCall);
        if (ttlColNullable && !ignoreTtlColNullable && ttlOnlyCleanUpNotNullRows) {
            ttlFilterForOnline = rexBuilder.makeCall(SqlStdOperatorTable.OR, ttlFilterForOnline,
                rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ttlColInputRefForOnline));
        }
        relBuilder.filter(ttlFilterForOnline);
        RelNode ttlOnelineFilter = relBuilder.build();
        ttlTableScanSet.add((TableScan) ttlOnelineFilter.getInput(0));

        if (this.ttlQueryType == TtlQueryType.HOT_ONLY) {
            return ttlOnelineFilter.accept(this);
        }

        relBuilder.clear();

        //任意选择一个arc cci
        String arcCciTableName = tableMeta.getArchiveColumnarIndexPublished().keySet().iterator().next();
        //创建归档表的扫描计划
        relBuilder.scan(arcCciTableName);
        RexInputRef ttlColInputRefForArchive = relBuilder.field(ttlColName);
        RexNode ttlFilterForArchive =
            rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ttlColInputRefForArchive, ttlQueryBoundaryCall);
        if (ttlColNullable && !ignoreTtlColNullable && !ttlOnlyCleanUpNotNullRows) {
            ttlFilterForArchive = rexBuilder.makeCall(SqlStdOperatorTable.OR, ttlFilterForOnline,
                rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ttlColInputRefForArchive));
        }
        relBuilder.filter(ttlFilterForArchive);
        RelNode ttlArchiveFilter = relBuilder.build();
        ttlTableScanSet.add((TableScan) ttlArchiveFilter.getInput(0));

        if (this.ttlQueryType == TtlQueryType.COLD_ONLY) {
            return ttlArchiveFilter.accept(this);
        }

        //创建并返回union计划
        LogicalUnion ttlUnion = LogicalHybridUnion.create(ImmutableList.of(ttlOnelineFilter, ttlArchiveFilter), true);
        //注意这里避免死循环
        return ttlUnion.accept(this);
    }

    private RelNode buildBlackHoleAccessTable(RelOptSchema catalog,
                                              TableScan scan,
                                              String schemaName,
                                              TableMeta tMeta) {
        final Map<String, GsiMetaManager.GsiIndexMetaBean> columnarIndexPublished = tMeta.getColumnarIndexPublished();
        if (!columnarIndexPublished.isEmpty()) {
            this.withIndexHint = true;
            this.lockMode = null;
            return buildForceIndex(catalog, scan, schemaName, tMeta, Engine.BLACKHOLE,
                TddlSqlToRelConverter.unwrapGsiName(columnarIndexPublished.keySet().iterator().next()),
                IndexUtil.IndexHintType.FORCE_INDEX);
        }
        return null;
    }

    /**
     * remove force index hint
     */
    protected void removeForceIndex(TableScan scan) {
        SqlNode indexNode = scan.getIndexNode();
        if (indexNode instanceof SqlNodeList) {
            SqlNodeList sqlNodeList = (SqlNodeList) indexNode;
            // use one new list in case of immutable list cannot be modified
            SqlNodeList newList = new SqlNodeList(sqlNodeList.getParserPosition());
            boolean needReset = false;

            for (SqlNode node : sqlNodeList.getList()) {
                if (isForceIndex(node)) {
                    needReset = true;
                } else {
                    newList.add(node);
                }
            }
            if (needReset) {
                if (newList.size() == 0) {
                    scan.setIndexNode(null);
                } else {
                    scan.setIndexNode(newList);
                }
            }
        }
    }

    private boolean isForceIndex(SqlNode node) {
        return node instanceof SqlIndexHint && ((SqlIndexHint) node).forceIndex();
    }

    protected RelNode buildForceIndexByIndexHint(RelOptSchema catalog, TableScan scan, String schemaName,
                                                 TableMeta tMeta,
                                                 Engine engine) {
        // get index hint
        Pair<SqlCall, IndexUtil.IndexHintType> pair = getIndexHint(scan);
        if (pair == null) {
            return null;
        }
        List<SqlNode> args = pair.getKey().getOperandList();
        String tablePart = args.get(1).toString();
        String indexPart = null;
        if (args.size() > 2) {
            indexPart = args.get(2).toString();
        }
        if (indexPart == null) {
            return buildForceIndex(catalog, scan, schemaName, tMeta, engine, tablePart, pair.getValue());
        } else {
            return buildForceIndex(catalog, scan, schemaName, tMeta, engine, tablePart, indexPart, pair.getValue());
        }
    }

    private Pair<SqlCall, IndexUtil.IndexHintType> getIndexHint(TableScan scan) {
        SqlNodeList sqlNodes = scan.getHints();
        if (sqlNodes == null) {
            return null;
        }
        for (SqlNode node : sqlNodes) {
            if (node instanceof SqlCall) {
                SqlCall call = (SqlCall) node;
                if (call.getOperator().getName().equalsIgnoreCase(HintType.CMD_INDEX.getValue().toLowerCase())) {
                    if (call.getOperandList().size() < 2) {
                        // throw new IllegalArgumentException("wrong args num in index hint");
                        return null;
                    }
                    return Pair.of(call, IndexUtil.IndexHintType.FORCE_INDEX);
                }
                if (call.getOperator().getName().equalsIgnoreCase(HintType.CMD_PAGING_INDEX.getValue().toLowerCase())) {
                    if (call.getOperandList().size() < 2) {
                        // throw new IllegalArgumentException("wrong args num in index hint");
                        return null;
                    }
                    return Pair.of(call, IndexUtil.IndexHintType.PAGING_FORCE_INDEX);
                }
            }
        }
        return null;
    }

    /**
     * Constructs a RelNode with the specified forced index applied.
     *
     * @param catalog Catalog object
     * @param scan Initial TableScan node
     * @param schemaName Schema name
     * @param tMeta Table metadata
     * @param engine Execution engine
     * @return RelNode with the enforced index
     */
    protected RelNode buildForceIndexByForceIndex(RelOptSchema catalog, TableScan scan, String schemaName,
                                                  TableMeta tMeta, Engine engine) {
        Pair<SqlIdentifier, IndexUtil.IndexHintType> pair = getForceIndex(scan);
        if (pair != null) {
            this.withIndexHint = true;
            String tablePart = pair.getKey().names.get(0);
            String indexPart = pair.getKey().names.size() > 1 ? pair.getKey().names.get(1) : null;

            if (indexPart == null) {
                return buildForceIndex(catalog, scan, schemaName, tMeta, engine, tablePart, pair.getValue());
            } else {
                return buildForceIndex(catalog, scan, schemaName, tMeta, engine, tablePart, indexPart, pair.getValue());
            }
        }
        return null;
    }

    /**
     * Builds a RelNode with the specified forced index applied. support gsi and main table local index
     * <p>
     * if indexName is primary or a local index in main table, return a logical view for main table with the same index node
     * if indexName is a gsi, return one gsi lookup node
     * otherwise, return null
     *
     * @param catalog Catalog object
     * @param scan Initial TableScan node
     * @param schemaName Schema name
     * @param tMeta Table metadata
     * @param engine Execution engine
     * @param indexName logical table index, might be a gsi or primary or local index
     * @return RelNode with the enforced index
     */
    AbstractRelNode buildForceIndex(RelOptSchema catalog, TableScan scan, String schemaName, TableMeta tMeta,
                                    Engine engine, @NotNull String indexName, IndexUtil.IndexHintType indexHintType) {
        GsiMetaManager.GsiIndexMetaBean gsi = tMeta.findGlobalSecondaryIndexByName(indexName);
        if (gsi == null) {
            System.out.println("test");
            // force index table.local_index
            IndexMeta localIndex = tMeta.findLocalIndexByName(indexName);
            if (localIndex != null) {
                hasLocalForceIndex = true;
                scan.setIndexNode(buildForceIndex(localIndex.getPhysicalIndexName(), indexHintType));
                return RelUtils.createLogicalView(scan, lockMode, engine);
            } else {
                return null;
            }
        } else {
            if (gsi.columnarIndex) {
                if (!(this.lockMode == null || this.lockMode == SqlSelect.LockMode.UNDEF)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_FORCE_COLUMNAR_INDEX, "unable to force cci with lock");
                }
                if (engine != Engine.BLACKHOLE && this.sqlKind == SqlKind.DELETE) {
                    throw new TddlRuntimeException(ErrorCode.ERR_FORCE_COLUMNAR_INDEX,
                        "unable to force cci with delete");
                }
                if (engine != Engine.BLACKHOLE && this.sqlKind == SqlKind.UPDATE) {
                    throw new TddlRuntimeException(ErrorCode.ERR_FORCE_COLUMNAR_INDEX,
                        "unable to force cci with update");
                }
                // support insert select and replace select
                return buildOSSTableScan(catalog, scan, schemaName, engine, gsi);
            } else {
                return buildLogicalTableLookup(catalog, scan, schemaName, engine, gsi, null, indexHintType);
            }
        }
    }

    /**
     * Builds a RelNode with the specified forced index applied. support gsi and its local index
     * <p>
     * if gsiPart was PRIMARY, return main table logicalview with index node of lsi/primary identified by lsiPart.
     * if gsiPart was a gsi, return gsi lookup node and the index scan node got the index node of lsi/primary identified by lsiPart.
     * <p>
     * if any index(gsiPart or lsiPart) was not found, return null
     *
     * @param catalog Catalog object
     * @param scan Initial TableScan node
     * @param schemaName Schema name
     * @param tMeta Table metadata
     * @param engine Execution engine
     * @param gsiPart gsi or primary
     * @param lsiPart physical table index, might be a primary or local index
     * @return RelNode with the enforced index
     */
    private AbstractRelNode buildForceIndex(RelOptSchema catalog, TableScan scan, String schemaName, TableMeta tMeta,
                                            Engine engine, @NotNull String gsiPart,
                                            @NotNull String lsiPart, IndexUtil.IndexHintType indexHintType) {
        // try gsi.local
        GsiMetaManager.GsiIndexMetaBean gsi = tMeta.findGlobalSecondaryIndexByName(gsiPart);
        if (gsi != null) {
            TableMeta gsiMeta = this.getPlannerContext().getExecutionContext().getSchemaManager(schemaName)
                .getTable(gsi.indexTableName);
            IndexMeta gsiLocalIndex = gsiMeta.findLocalIndexByName(lsiPart);
            if (gsiLocalIndex == null) {
                // error gsi local index doesn't exist
                return null;
            } else {
                if (gsi.columnarIndex) {
                    // CCI should not reach here since CCI must not have local index
                    return null;
                } else {
                    hasLocalForceIndex = true;
                    return buildLogicalTableLookup(catalog, scan, schemaName, engine, gsi, lsiPart, indexHintType);
                }
            }
        }

        // try primary.local index
        if (GeneralUtil.isPrimary(gsiPart)) {
            // try primary.local
            IndexMeta primaryLocalIndex = tMeta.findLocalIndexByName(lsiPart);
            if (primaryLocalIndex != null) {
                hasLocalForceIndex = true;
                scan.setIndexNode(buildForceIndex(primaryLocalIndex.getPhysicalIndexName(), indexHintType));
            } else {
                // local index were not found any match for tblPath
                return null;
            }
            hasLocalForceIndex = true;
            // force index(primary) equals force index(primary.primary)
            LogicalView lv = RelUtils.createLogicalView(scan, lockMode, engine);
            lv.setFromForceIndex(true);
            return lv;
        }
        return null;
    }

    /**
     * Builds a LogicalTableLookup based on the provided components.
     *
     * @param catalog Catalog object
     * @param scan Original table scan
     * @param schemaName Schema name
     * @param engine Execution engine
     * @param gsi Global Secondary Index metadata
     * @param localIndexPath Local index path (if any)
     * @return A new LogicalTableLookup instance
     */
    protected LogicalTableLookup buildLogicalTableLookup(RelOptSchema catalog, TableScan scan, String schemaName,
                                                         Engine engine, GsiMetaManager.GsiIndexMetaBean gsi,
                                                         String localIndexPath, IndexUtil.IndexHintType indexHintType) {
        // don't support paging force gsi
        if (StringUtils.isEmpty(localIndexPath) && indexHintType == IndexUtil.IndexHintType.PAGING_FORCE_INDEX) {
            return null;
        }
        // Create a logical view from the original table scan, and remove its index node
        scan.setIndexNode(null);
        LogicalView primary = RelUtils.createLogicalView(scan, lockMode, engine);

        // Obtain the index table and create a corresponding table scan
        RelOptTable indexTable = catalog.getTableForMember(ImmutableList.of(schemaName, gsi.indexName));
        LogicalTableScan indexTableScan = LogicalTableScan.create(scan.getCluster(),
            indexTable,
            scan.getHints(),
            StringUtils.isEmpty(localIndexPath) ? null : buildForceIndex(localIndexPath, indexHintType),
            scan.getFlashback(),
            scan.getFlashbackOperator(),
            null);

        // Create index lookup scan node
        final LogicalIndexScan index = new LogicalIndexScan(gsi.tableName, indexTable, indexTableScan, this.lockMode);
        this.withIndexHint = true;
        return RelUtils.createTableLookup(primary, index, index.getTable());
    }

    @NotNull
    OSSTableScan buildOSSTableScan(RelOptSchema catalog, TableScan scan, String schemaName, Engine engine,
                                   GsiMetaManager.GsiIndexMetaBean gsi) {
        final RelOptTable indexTable = catalog.getTableForMember(ImmutableList.of(schemaName, gsi.indexName));
        final LogicalTableScan columnarTableScan = LogicalTableScan.create(
            scan.getCluster(),
            indexTable,
            scan.getHints(),
            null,
            scan.getFlashback(),
            scan.getFlashbackOperator(),
            null
        );
        this.withIndexHint = true;
        this.existForceColumnar = true;
        scan.setIndexNode(null);
        return new OSSTableScan(columnarTableScan, this.lockMode);
    }

    /**
     * Constructs a SQL node list containing a force index hint for the specified local index path.
     *
     * @param localIndexPath The path of the local index to be forced in the query
     * @return A SQL node list with the force index hint
     */
    @NotNull
    private SqlNodeList buildForceIndex(@NotNull String localIndexPath, IndexUtil.IndexHintType indexHintType) {
        if (indexHintType == IndexUtil.IndexHintType.PAGING_FORCE_INDEX) {
            this.existPagingForce = true;
        }
        return new SqlNodeList(
            Collections.singletonList(
                new SqlIndexHint(
                    SqlCharStringLiteral.createCharString(
                        indexHintType == IndexUtil.IndexHintType.PAGING_FORCE_INDEX ?
                            "PAGING_FORCE INDEX" : "FORCE INDEX", ZERO),
                    null,
                    SqlNodeList.of(new SqlIdentifier(localIndexPath, ZERO)),
                    ZERO)
            ),
            ZERO
        );
    }

    /**
     * get force index info, or return null if not exists
     *
     * @param scan Original table scan
     * @return force index info, null if there is no force index in table scan
     */
    protected Pair<SqlIdentifier, IndexUtil.IndexHintType> getForceIndex(TableScan scan) {
        if (!(scan.getIndexNode() instanceof SqlNodeList)) {
            return null;
        }

        SqlNodeList indexNodes = (SqlNodeList) scan.getIndexNode();
        if (indexNodes == null || indexNodes.size() == 0) {
            return null;
        }

        // get first force index hint
        SqlIndexHint sqlIndexHint = null;
        for (SqlNode node : indexNodes) {
            if (node instanceof SqlIndexHint &&
                (((SqlIndexHint) node).forceIndex() || ((SqlIndexHint) node).pagingForceIndex())) {
                sqlIndexHint = (SqlIndexHint) node;
                break;
            }
        }

        if (sqlIndexHint == null) {
            return null;
        }

        SqlNodeList indexList = sqlIndexHint.getIndexList();

        if (indexList != null &&
            indexList.size() > 0 &&
            indexList.get(0) instanceof SqlIdentifier) {
            return Pair.of((SqlIdentifier) indexList.get(0),
                sqlIndexHint.pagingForceIndex() ?
                    IndexUtil.IndexHintType.PAGING_FORCE_INDEX : IndexUtil.IndexHintType.FORCE_INDEX);
        }

        return null;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        existsIntersect = true;
        return super.visit(intersect);
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        existsMinus = true;
        return super.visit(minus);
    }

    @Override
    public final RelNode visit(LogicalProject project) {
        ReplaceTableScanInFilterSubQueryFinder
            replaceTableScanInFilterSubQueryFinder = new ReplaceTableScanInFilterSubQueryFinder(sqlKind,
            lockMode,
            allTableSingleWithSameGroup,
            allTableBroadcast,
            allTableSingleNoBroadcast,
            singleDbIndex,
            schemaNames,
            plannerContext);
        List<RexNode> rexNodeList = Lists.newArrayList();
        for (RexNode r : project.getProjects()) {
            if (r instanceof RexCall) {
                existsWindow |= containsWindowExpr((RexCall) r);
            }
            existsNonPushDownFunc |= RexUtil.containsUnPushableFunctionForDirectPlan(r, mysql80);
            RexNode rexNode = r.accept(replaceTableScanInFilterSubQueryFinder);
            if (replaceTableScanInFilterSubQueryFinder.baseLogicalView != null && baseLogicalView == null) {
                baseLogicalView = replaceTableScanInFilterSubQueryFinder.baseLogicalView;
            }
            if (replaceTableScanInFilterSubQueryFinder.tableNames.size() > 0) {
                tableNames.addAll(replaceTableScanInFilterSubQueryFinder.tableNames);
                tableStorages.addAll(replaceTableScanInFilterSubQueryFinder.storageIds);
            }

            if (allTableSingle) {
                if (!replaceTableScanInFilterSubQueryFinder.isAllSingleTable()) {
                    allTableSingle = false;
                }
            }

            if (allTableSingleWithSameGroup) {

                if (!replaceTableScanInFilterSubQueryFinder.isAllSingleTableWithSameGroup()) {
                    allTableSingleWithSameGroup = false;
                } else {
                    // singleDbIndex might be null before.
                    singleDbIndex = replaceTableScanInFilterSubQueryFinder.getSingleDbIndex();
                }
                if (this.allTableSingleTgId == null) {
                    this.allTableSingleTgId = replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId();
                } else if (replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId() != null &&
                    !this.allTableSingleTgId.equals(replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId())) {
                    allTableSingleWithSameGroup = false;
                }
            }
            if (this.allTableBroadcast && !replaceTableScanInFilterSubQueryFinder.isAllTableBroadcast()) {
                this.allTableBroadcast = false;
            }
            this.commonGroupKeyInfo.updateCommonGroupKeyByGroupInfo(
                replaceTableScanInFilterSubQueryFinder.getPlanCommonGroupInfo());
            if (this.commonGroupKeyInfo.isContainAnyReplicasTables()) {
                this.allTableBroadcast = false;
                this.allTableSingleNoBroadcast = false;
                this.allTableSingle = false;
                this.allTableHaveColumnar = false;
            }

            rexNodeList.add(rexNode);
        }
        RelNode logicalProject = super.visit(project);
        RelMetadataQuery mq = logicalProject.getCluster().getMetadataQuery();
        this.containUncertainValue |= replaceTableScanInFilterSubQueryFinder.isContainUncertainValue();
        this.containComplexExpression |= replaceTableScanInFilterSubQueryFinder.isContainComplexExpression();
        this.existsWindow |= replaceTableScanInFilterSubQueryFinder.isExistsWindow();
        this.existsNonPushDownFunc |= replaceTableScanInFilterSubQueryFinder.isExistsNonPushDownFunc();
        this.existsOSSTable |= replaceTableScanInFilterSubQueryFinder.existsOSSTable();
        this.existPagingForce |= replaceTableScanInFilterSubQueryFinder.isExistPagingForce();
        this.existForceColumnar |= replaceTableScanInFilterSubQueryFinder.isExistForceColumnar();
        this.allTableHaveColumnar &= replaceTableScanInFilterSubQueryFinder.isAllTableHaveColumnar();
        return LogicalProject.create(logicalProject.getInput(0),
            rexNodeList,
            logicalProject.getRowType(),
            mq.getOriginalRowType(logicalProject),
            logicalProject.getVariablesSet()).setHints(project.getHints());
    }

    private boolean containsWindowExpr(RexCall rexCall) {
        return rexCall instanceof RexOver || rexCall.getOperands().stream()
            .anyMatch(t -> t instanceof RexCall && containsWindowExpr((RexCall) t));
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        existsNonPushDownFunc |= RexUtil.containsUnPushableFunctionForDirectPlan(filter.getCondition(), mysql80);
        ReplaceTableScanInFilterSubQueryFinder
            replaceTableScanInFilterSubQueryFinder = new ReplaceTableScanInFilterSubQueryFinder(sqlKind,
            lockMode,
            allTableSingleWithSameGroup,
            allTableBroadcast,
            allTableSingleNoBroadcast,
            singleDbIndex,
            schemaNames,
            plannerContext);
        RexNode rexNode = filter.getCondition().accept(replaceTableScanInFilterSubQueryFinder);
        RelNode logicalFilter = super.visit(filter);

        if (replaceTableScanInFilterSubQueryFinder.baseLogicalView != null && baseLogicalView == null) {
            baseLogicalView = replaceTableScanInFilterSubQueryFinder.baseLogicalView;
        }
        if (replaceTableScanInFilterSubQueryFinder.tableNames.size() > 0) {
            tableNames.addAll(replaceTableScanInFilterSubQueryFinder.tableNames);
            tableStorages.addAll(replaceTableScanInFilterSubQueryFinder.storageIds);
        }

        if (allTableSingle) {
            if (!replaceTableScanInFilterSubQueryFinder.isAllSingleTable()) {
                allTableSingle = false;
            }
        }

        if (allTableSingleWithSameGroup) {
            if (!replaceTableScanInFilterSubQueryFinder.isAllSingleTableWithSameGroup()) {
                allTableSingleWithSameGroup = false;
            } else {
                // singleDbIndex might be null before.
                singleDbIndex = replaceTableScanInFilterSubQueryFinder.getSingleDbIndex();
            }
            if (this.allTableSingleTgId == null) {
                this.allTableSingleTgId = replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId();
            } else if (replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId() != null &&
                !this.allTableSingleTgId.equals(replaceTableScanInFilterSubQueryFinder.getAllTableSingleTgId())) {
                allTableSingleWithSameGroup = false;
            }
        }

        if (this.allTableBroadcast && !replaceTableScanInFilterSubQueryFinder.isAllTableBroadcast()) {
            this.allTableBroadcast = false;
        }

        if (this.allTableSingleNoBroadcast && !replaceTableScanInFilterSubQueryFinder.isAllTableSingleNoBroadcast()) {
            this.allTableSingleNoBroadcast = false;
        }

        this.commonGroupKeyInfo.updateCommonGroupKeyByGroupInfo(
            replaceTableScanInFilterSubQueryFinder.getPlanCommonGroupInfo());
        if (this.commonGroupKeyInfo.isContainAnyReplicasTables()) {
            this.allTableBroadcast = false;
            this.allTableSingleNoBroadcast = false;
            this.allTableSingle = false;
            this.allTableHaveColumnar = false;
        }

        this.containUncertainValue |= replaceTableScanInFilterSubQueryFinder.isContainUncertainValue();
        this.containComplexExpression |= replaceTableScanInFilterSubQueryFinder.isContainComplexExpression();
        this.existsNonPushDownFunc |= replaceTableScanInFilterSubQueryFinder.isExistsNonPushDownFunc();
        this.existsOSSTable |= replaceTableScanInFilterSubQueryFinder.existsOSSTable();
        this.existPagingForce |= replaceTableScanInFilterSubQueryFinder.isExistPagingForce();
        this.existForceColumnar |= replaceTableScanInFilterSubQueryFinder.isExistForceColumnar();
        this.allTableHaveColumnar &= replaceTableScanInFilterSubQueryFinder.isAllTableHaveColumnar();
        this.existsWindow |= replaceTableScanInFilterSubQueryFinder.isExistsWindow();
        return filter.copy(logicalFilter.getTraitSet(), logicalFilter.getInput(0), rexNode).setHints(filter.getHints());
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        existsNonPushDownFunc |= RexUtil.containsUnPushableFunctionForDirectPlan(join.getCondition(), mysql80);
        ReplaceTableScanInFilterSubQueryFinder
            replaceTableScanInFilterSubQueryFinder = new ReplaceTableScanInFilterSubQueryFinder(sqlKind,
            lockMode,
            allTableSingleWithSameGroup,
            allTableBroadcast,
            allTableSingleNoBroadcast,
            singleDbIndex,
            schemaNames,
            plannerContext);
        RexNode rexNode = join.getCondition().accept(replaceTableScanInFilterSubQueryFinder);
        LogicalJoin logicalJoin = (LogicalJoin) super.visit(join);

        if (replaceTableScanInFilterSubQueryFinder.baseLogicalView != null && baseLogicalView == null) {
            baseLogicalView = replaceTableScanInFilterSubQueryFinder.baseLogicalView;
        }
        if (replaceTableScanInFilterSubQueryFinder.tableNames.size() > 0) {
            tableNames.addAll(replaceTableScanInFilterSubQueryFinder.tableNames);
            tableStorages.addAll(replaceTableScanInFilterSubQueryFinder.storageIds);
        }

        this.commonGroupKeyInfo.updateCommonGroupKeyByGroupInfo(
            replaceTableScanInFilterSubQueryFinder.getPlanCommonGroupInfo());
        if (this.commonGroupKeyInfo.isContainAnyReplicasTables()) {
            this.allTableBroadcast = false;
            this.allTableSingleNoBroadcast = false;
            this.allTableSingle = false;
            this.allTableHaveColumnar = false;
        }
        return logicalJoin.copy(logicalJoin.getTraitSet(), rexNode, logicalJoin.getLeft(),
            logicalJoin.getRight(), logicalJoin.getJoinType(), logicalJoin.isSemiJoinDone());
    }

    @Override
    public final RelNode visit(RelNode other) {
        if ((other instanceof LogicalTableModify)) {
            this.allTableHaveColumnar = false;
            LogicalTableModify modify = (LogicalTableModify) super.visit(other);
            setShouldRemoveSchemaName(modify.getTable().getQualifiedName());
            TableModify.Operation operation = modify.getOperation();

            CheckModifyLimitation.check(modify, plannerContext);

            final boolean modifyFkReferenced = CheckModifyLimitation.checkModifyFkReferenced(modify,
                this.plannerContext.getExecutionContext());

            TableModify newPlan;
            Map<String, TableProperties> targetTableProperties;
            Map<String, TableProperties> refTableProperties;
            if (operation == TableModify.Operation.INSERT || operation == TableModify.Operation.REPLACE) {
                // Derive schema/table from the target table reference.
                final List<String> qn = modify.getTable().getQualifiedName();
                final String insertSchemaName = qn.size() >= 2 ? qn.get(0) :
                    this.plannerContext.getExecutionContext().getSchemaName();
                final String insertTableName = qn.get(qn.size() - 1);

                // For EXTERNAL engine tables, short-circuit to LogicalExternalInsert.
                final TableMeta insertTableMeta = this.plannerContext.getExecutionContext()
                    .getSchemaManager(insertSchemaName).getTable(insertTableName);
                if (insertTableMeta.getEngine() == Engine.EXTERNAL) {
                    this.existsOSSTable = true;
                    final RelOptTable relOptTable = modify.getTable();
                    TableSink tableSink = null;
                    String catalogName = insertTableMeta.getExternalCatalogName();
                    if (catalogName != null) {
                        ExternalCatalogInfo catInfo = ExternalCatalogManager.getInstance().get(catalogName);
                        if (catInfo != null) {
                            Map<String, String> sinkOpts = Maps.newHashMap(catInfo.getProperties());
                            sinkOpts.put("connector", catInfo.getConnector());
                            ConnectorDescriptor sinkFactory = ConnectorRegistry.getInstance()
                                .getOrNull(catInfo.getConnector());
                            if (sinkFactory != null) {
                                tableSink = sinkFactory.createTableSink(sinkOpts, relOptTable)
                                    .orElse(null);
                            }
                        }
                    }
                    if (tableSink == null) {
                        tableSink = new TableSink.DefaultTableSink(relOptTable);
                    }
                    final LogicalExternalInsert externalInsert =
                        LogicalExternalInsert.create(modify, tableSink);
                    plannerContext.setHasExternalTableOperation(true);
                    if (!schemaNames.contains(insertSchemaName)) {
                        schemaNames.add(insertSchemaName);
                    }
                    return externalInsert.setHints(modify.getHints());
                }

                LogicalInsert logicalInsert = new LogicalInsert(modify);
                String schemaName = logicalInsert.getSchemaName();
                String tableName = logicalInsert.getLogicalTableName();

                if (!schemaNames.contains(schemaName)) {
                    schemaNames.add(schemaName);
                }

                // input of LogicalInsert (like LogicalProject) may be removed
                // by planner rules, but its row type must be saved.
                logicalInsert.setInsertRowType(logicalInsert.getInput().getRowType());

                // get tableMeta
                TableMeta tableMeta = this.plannerContext.getExecutionContext().getSchemaManager(schemaName)
                    .getTable(tableName);
                this.modifyExternalizedData |= ExternalizedDmlRewriter.needsHandling(tableMeta);

                List<String> autoIncColumns = tableMeta.getAutoIncrementColumns();
                Collection<ColumnMeta> pk = tableMeta.getPrimaryKey();

                if (pk.size() == 1 && autoIncColumns.size() == 1) {
                    String pkName = pk.iterator().next().getName();
                    String autoIncName = autoIncColumns.iterator().next();

                    // pk为自增列且用户未指定pk值(或显式指定为null)，可跳过pk检查
                    if (StringUtils.equalsIgnoreCase(pkName, autoIncName)) {
                        boolean canSkip = checkAutoIncColumnCanSkipPkCheck(logicalInsert, pkName);
                        if (canSkip) {
                            logicalInsert.setCanSkipPkCheck(true);
                        }
                    }
                }

                // insertion into broadcast table can't be transformed to
                // DirectTableOperation.
                if (OptimizerContext.getContext(schemaName).getRuleManager().isBroadCastOrReplicas(tableName)
                    || SequenceManagerProxy.getInstance().isUsingSequence(schemaName, tableName)) {
                    allTableSingleWithSameGroup = false;
                }

                if (modify.isSourceSelect()) {
                    final ExecutionContext ec = plannerContext.getExecutionContext();
                    logicalInsert = LogicalWriteUtil.handleDynamicImplicitDefault(logicalInsert, ec);
                }

                newPlan = logicalInsert;
                targetTableProperties = RelUtils.buildTablePropertiesMap(logicalInsert.getTargetTableNames(),
                    schemaName, this.plannerContext.getExecutionContext());
                refTableProperties = new HashMap<>(targetTableProperties);

                // Remove #allTableSingle flag for case that single tables not all in one table group
                refTableProperties
                    .values()
                    .stream()
                    .filter(tp -> null != tp.getPartInfo())
                    .forEach(tp -> updateAllTableSingleWithSameTgFlag(tp.getPartInfo()));

                this.modifyShardingColumn |= CheckModifyLimitation.checkUpsertModifyShardingColumn(logicalInsert,
                    this.plannerContext);

                if (modifyFkReferenced) {
                    logicalInsert.setModifyForeignKey(true);
                }

                if (modify.isSourceSelect() && !this.plannerContext.getExecutionContext().getParamManager()
                    .getBoolean(ConnectionParams.ENABLE_INSERT_SELECT_WITH_FLASHBACK_PUSH_DOWN)) {
                    if (RelUtils.containFlashback(logicalInsert.getInput())) {
                        insertSelectWithFlashback = true;
                    }
                }

            } else { // UPDATE / DELETE
                // Currently we do not allow create GSI on broadcast or single table
                targetTableProperties = new HashMap<>();
                refTableProperties = new HashMap<>();
                for (RelOptTable table : modify.getTableInfo().getTargetTableSet()) {
                    final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
                    targetTableProperties.putAll(RelUtils.buildTablePropertiesMap(ImmutableList.of(qn.right), qn.left,
                        this.plannerContext.getExecutionContext()));

                    if (TStringUtil.isNotBlank(qn.left) && !schemaNames.contains(qn.left)) {
                        schemaNames.add(qn.left);
                    }
                }

                for (RelOptTable table : modify.getTableInfo().getRefTables()) {
                    final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
                    refTableProperties.putAll(RelUtils.buildTablePropertiesMap(ImmutableList.of(qn.right), qn.left,
                        this.plannerContext.getExecutionContext()));
                }

                // Remove #allTableSingle flag for case that single tables not all in one table group
                refTableProperties
                    .values()
                    .stream()
                    .filter(tp -> null != tp.getPartInfo())
                    .forEach(tp -> updateAllTableSingleWithSameTgFlag(tp.getPartInfo()));

                if (this.allTableSingleWithSameGroup && !refTableProperties.isEmpty()) {
                    final boolean targetAllBroadcast = RelUtils.allTableBroadcast(targetTableProperties);
                    final boolean targetNoBroadcast = RelUtils.allTableNotBroadcast(targetTableProperties);
                    final boolean refAllBroadcast = RelUtils.allTableBroadcast(refTableProperties);

                    this.allTableSingleWithSameGroup = (targetAllBroadcast && refAllBroadcast) || targetNoBroadcast;
                }

                if ((ast instanceof SqlDelete && ((SqlDelete) ast).getOffset() != null) ||
                    (ast instanceof SqlUpdate && ((SqlUpdate) ast).getOffset() != null)) {
                    modifyWithLimitOffset = true;
                }

                final LogicalModify logicalModify = new LogicalModify(modify);
                detectExternalizedUpdate(logicalModify, targetTableProperties);
                this.modifyShardingColumn |=
                    CheckModifyLimitation.checkModifyShardingColumn(logicalModify, this.plannerContext);

                if (CheckModifyLimitation.checkModifyFkReferencing(logicalModify,
                    this.plannerContext.getExecutionContext()) ||
                    modifyFkReferenced) {
                    this.modifyForeignKey = true;
                }

                if (modifyFkReferenced
                    || logicalModify.isUpdate() && CheckModifyLimitation.checkModifyForeignKeyConstraint(
                    logicalModify, this.plannerContext.getExecutionContext())) {
                    logicalModify.setModifyForeignKey(true);
                }

                logicalModify.setOriginalSqlNode(ast);

                newPlan = logicalModify;
            }

            List<String> modifyingTableNames = Lists.newArrayList(targetTableProperties.keySet());
            this.modifiedTables = ImmutableList.copyOf(targetTableProperties.values());

            updateTableProperties(refTableProperties, newPlan);

            if (!modifyBroadcastTable && RelUtils.containsBroadcastTable(targetTableProperties, modifyingTableNames)) {
                modifyBroadcastTable = true;
            }

            if (!modifyGsiTable && RelUtils.containsGsiTable(targetTableProperties, modifyingTableNames)) {
                modifyGsiTable = true;
            }

            if (!containScaleOutWritableTable && RelUtils
                .containScaleOutWriableTable(targetTableProperties, modifyingTableNames,
                    this.plannerContext.getExecutionContext())) {
                containScaleOutWritableTable = true;
            }

            if (!containReplicateWriableTable && RelUtils
                .containsReplicateWriableTable(targetTableProperties, modifyingTableNames,
                    this.plannerContext.getExecutionContext())) {
                containReplicateWriableTable = true;
            }

            if (!containOnlineModifyColumnTable && RelUtils.containOnlineModifyColumnTable(targetTableProperties,
                modifyingTableNames, this.plannerContext.getExecutionContext())) {
                containOnlineModifyColumnTable = true;
            }

            if (!containGeneratedColumn && RelUtils.containGeneratedColumn(targetTableProperties,
                modifyingTableNames, this.plannerContext.getExecutionContext())) {
                containGeneratedColumn = true;
            }

            return newPlan.setHints(modify.getHints());
        } else if (other instanceof DDL) {
            return convertToLogicalDdlPlan((DDL) other);
        } else if (other instanceof LogicalRecyclebin) {
            return other;
        } else if (other instanceof Dal) {
            final Dal dalNode = (Dal) other;
            final SqlDal sqlDal = dalNode.getAst();
            String schemaName = null;
            SqlKind kind = sqlDal.getKind();
            if (sqlDal instanceof SqlShow) {
                kind = ((SqlShow) sqlDal).getShowKind();
            }
            if (kind.belongsTo(SqlKind.LOGICAL_SHOW_WITH_SCHEMA) && kind == SqlKind.SHOW_TABLES) {
                String fromScehma = ((SqlShowTables) sqlDal).getSchema();
                if (!TStringUtil.equalsIgnoreCase(fromScehma, "information_schema") && !TStringUtil
                    .equalsIgnoreCase(fromScehma, "mysql")) {
                    schemaName = ((SqlShowTables) sqlDal).getSchema();
                } else if (TStringUtil
                    .equalsIgnoreCase(fromScehma, "information_schema")) {
                    schemaName = ((SqlShowTables) sqlDal).getSchema();
                }
            } else if (kind.belongsTo(SqlKind.LOGICAL_SHOW_WITH_SCHEMA) && kind == SqlKind.SHOW_LOCALITY_INFO) {
                String fromSchema = ((SqlShowLocalityInfo) sqlDal).getSchema();
                if (!TStringUtil.equalsIgnoreCase(fromSchema, "information_schema") && !TStringUtil
                    .equalsIgnoreCase(fromSchema, "mysql")) {
                    schemaName = ((SqlShowLocalityInfo) sqlDal).getSchema();
                } else if (TStringUtil.equalsIgnoreCase(fromSchema, "information_schema")) {
                    schemaName = ((SqlShowLocalityInfo) sqlDal).getSchema();
                }
            } else if (kind.belongsTo(SqlKind.LOGICAL_SHOW_WITH_SCHEMA) && kind == SqlKind.SHOW_PHYSICAL_DDL) {
                String fromSchema = ((SqlShowPhysicalDdl) sqlDal).getSchema();
                if (!TStringUtil.equalsIgnoreCase(fromSchema, "information_schema") && !TStringUtil
                    .equalsIgnoreCase(fromSchema, "mysql")) {
                    schemaName = ((SqlShowPhysicalDdl) sqlDal).getSchema();
                } else if (ConfigDataMode.isPolarDbX() && TStringUtil
                    .equalsIgnoreCase(fromSchema, "information_schema")) {
                    schemaName = ((SqlShowPhysicalDdl) sqlDal).getSchema();
                }
            } else if (kind.belongsTo(SqlKind.LOGICAL_SHOW_WITH_TABLE)) {
                if (sqlDal.getTableName() instanceof SqlIdentifier
                    && ((SqlIdentifier) sqlDal.getTableName()).names.size() == 2) {
                    String schemaNameInTable = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    if (!TStringUtil.equalsIgnoreCase("information_schema", schemaNameInTable) && !TStringUtil
                        .equalsIgnoreCase("mysql", schemaNameInTable)) {
                        schemaName = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    } else if (TStringUtil.equalsIgnoreCase("information_schema", schemaNameInTable)) {
                        schemaName = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    }
                }
            } else if (kind == SqlKind.SHOW_INDEX) {
                if (sqlDal.getDbName() != null
                    && !TStringUtil.equalsIgnoreCase("information_schema", sqlDal.getDbName().toString())
                    && !TStringUtil.equalsIgnoreCase("mysql", sqlDal.getDbName().toString())) {
                    schemaName = sqlDal.getDbName().toString();
                } else if (sqlDal.getDbName() != null && TStringUtil
                    .equalsIgnoreCase("information_schema", sqlDal.getDbName().toString())) {
                    schemaName = sqlDal.getDbName().toString();
                }
            } else {
                if (sqlDal.getTableName() instanceof SqlIdentifier
                    && ((SqlIdentifier) sqlDal.getTableName()).names.size() == 2) {
                    String schemaNameInTable = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    if (!TStringUtil.equalsIgnoreCase("information_schema", schemaNameInTable) && !TStringUtil
                        .equalsIgnoreCase("mysql", schemaNameInTable)) {
                        schemaName = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    } else if (TStringUtil.equalsIgnoreCase("information_schema", schemaNameInTable)) {
                        schemaName = ((SqlIdentifier) sqlDal.getTableName()).names.get(0);
                    }
                }
            }
            // Support cross schema DAL
            final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
            if (optimizerContext == null) {
                GeneralUtil.nestedException("Cannot find schema: " + schemaName + ", please check your sql again.");
            }
            if (optimizerContext.isExternalSchema()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, "DAL unsupported for " + schemaName);
            }
            final TddlRuleManager rule = optimizerContext.getRuleManager();
            final boolean singleDbIndex = rule.isSingleDbIndex();
            String dbIndex = rule.getDefaultDbIndex(null);

            String phyTable = "";
            PartitionInfoManager partInfoMgr = optimizerContext.getPartitionInfoManager();
            int dbIndexMode = LogicalShow.DB_INDEX_MODE_NORMAL;
            if (null != sqlDal.getTableName()) {
                String logicalTable = RelUtils.lastStringValue(sqlDal.getTableName());
                PartitionInfo partInfo =
                    partInfoMgr.getPartitionInfo(logicalTable);
                boolean isSchemaValid =
                    !RelUtils.informationSchema(sqlDal.getTableName()) && !RelUtils.mysqlSchema(sqlDal.getTableName());
                if (!singleDbIndex && partInfo == null) {
                    TargetDB target = rule.shardAny(logicalTable);
                    if (isSchemaValid) {
                        phyTable = target.getTableNames().iterator().next();
                    }
                    dbIndexMode = DB_INDEX_MODE_RANDOM;
                    dbIndex = target.getDbIndex();
                } else if (partInfo != null) {
                    int[] modeHolder = new int[1];
                    PhysicalPartitionInfo prunedPartitionInfo =
                        resolvePartitionedTableDbIndex(sqlDal, partInfoMgr, logicalTable, modeHolder);
                    dbIndexMode = modeHolder[0];
                    dbIndex = prunedPartitionInfo.getGroupKey();
                    if (isSchemaValid) {
                        phyTable = prunedPartitionInfo.getPhyTable();
                    }
                }
            } else {
                int[] modeHolder = new int[] {dbIndexMode};
                String resolvedDbIndex = resolveNoTableNameDbIndex(sqlDal, schemaName, dbIndex, modeHolder);
                dbIndexMode = modeHolder[0];
                dbIndex = resolvedDbIndex;
            }
            if (kind.belongsTo(SqlKind.LOGICAL_SHOW_QUERY)) {
                final LogicalShow logicalShow =
                    LogicalShow.create((Show) other, dbIndex, phyTable, schemaName, dbIndexMode);
                final SqlNode dbName = ((Show) other).getAst().getDbName();
                if (null != dbName && (TStringUtil.equalsIgnoreCase("information_schema", RelUtils.stringValue(dbName))
                    || TStringUtil.equalsIgnoreCase("mysql", RelUtils.stringValue(dbName)))) {
                    logicalShow.setRemoveDbPrefix(false);
                }
                return logicalShow;
            } else if (kind.belongsTo(SqlKind.LOGICAL_SHOW_BINLOG)) {
                final LogicalShow logicalShow =
                    LogicalShow.create((Show) other, dbIndex, phyTable, schemaName, dbIndexMode);
                return logicalShow;
            } else if (kind == SqlKind.SHOW) {
                if (singleDbIndex && sqlDal.getTableName() != null) {
                    phyTable = RelUtils.lastStringValue(sqlDal.getTableName());
                }
                final PhyShow phyShow = PhyShow.create((Show) other, dbIndex, phyTable, schemaName);
                final SqlNode dbName = ((Show) other).getAst().getDbName();
                if (null != dbName && (TStringUtil.equalsIgnoreCase("information_schema", RelUtils.stringValue(dbName))
                    || TStringUtil.equalsIgnoreCase("mysql", RelUtils.stringValue(dbName)))) {
                    phyShow.setRemoveDbPrefix(false);
                }
                phyShow.setDbIndexMode(dbIndexMode);
                return phyShow;
            } else if (kind.belongsTo(SqlKind.SQL_SET_QUERY)) {
                return LogicalSet.create(dalNode, dbIndex, phyTable);
            } else if (kind == SqlKind.MOVE_DATABASE) {
                return LogicalReplicateDatabase.create(dalNode);
            } else {
                switch (kind) {
                case OPTIMIZE_TABLE:
                    return handleOptimizeTable(dalNode);
                case CHECK_TABLE_ROUTING:
                    return LogicalCheckTableRouting.create(dalNode);
                case LOCK_TABLE:
                case UNLOCK_TABLE:
                    return EmptyOperation.create(other.getCluster(), dalNode.getRowType());
                case BASELINE:
                    return LogicalBaseline.create(dalNode);
                case WARMUP:
                    return LogicalWarmup.create(dalNode);
                case WARMUP_CONTROL:
                    return LogicalWarmupControl.create(dalNode);
                case CREATE_ROUTING_RULE:
                case DROP_ROUTING_RULE:
                    return LogicalRoutingRule.create(dalNode);
                case CREATE_CCL_RULE:
                case SHOW_CCL_RULE:
                case DROP_CCL_RULE:
                case CLEAR_CCL_RULES:
                case CREATE_CCL_BLOCKER:
                case SHOW_CCL_BLOCKER:
                case DROP_CCL_BLOCKER:
                case CLEAR_CCL_BLOCKERS:
                case SLOW_SQL_CCL:
                    return LogicalCcl.create(dalNode);
                case ALTER_SYSTEM_REFRESH_STORAGE:
                    return LogicalAlterSystemRefreshStorage.create(dalNode);
                case ALTER_SYSTEM_RELOAD_STORAGE:
                    return LogicalAlterSystemReloadStorage.create(dalNode);
                case ALTER_SYSTEM_LEADER:
                    return LogicalAlterSystemLeader.create(dalNode);
                case REFRESH_EXTERNAL_CATALOG:
                    return LogicalDal.create(dalNode, dbIndex, phyTable, schemaName);
                default:
                    return LogicalDal.create(dalNode, dbIndex, phyTable, null);
                }
            }
        } else if (other instanceof DynamicValues) {
            if (!InstanceVersion.isMYSQL80() || !plannerContext.getExecutionContext().getParamManager()
                .getBoolean(ConnectionParams.ENABLE_VALUES_PUSHDOWN)) {
                this.existsUnPushedDynamicValues = true;
            }
            return super.visit(other);
        } else if (other instanceof RecursiveCTE) {
            this.plannerContext.setHasRecursiveCte(true);
            return super.visit(other);
        } else {
            return super.visit(other);
        }
    }

    private RelNode convertToLogicalDdlPlan(DDL ddl) {
        if (isSupportedByNewDdlEngine(ddl)) {
            // The plan will be executed via new DDL Engine.
            if (ddl instanceof CreateTable) {
                SqlCreateTable sqlCreateTable = (SqlCreateTable) ddl.getSqlNode();
                if (!sqlCreateTable.getAddedForeignKeys().isEmpty()) {
                    ForeignKeyData foreignKeyData = sqlCreateTable.getAddedForeignKeys().get(0);

                    SqlIdentifier tbNameId = (SqlIdentifier) sqlCreateTable.getName();
                    Pair<String, String> dbAndTb = CalciteUtils.getDbNameAndTableNameByTableIdentifier(tbNameId);
                    String dbName = dbAndTb.getKey();
                    String tbName = dbAndTb.getValue();

                    final List<Pair<String, ForeignKeyData>> refTables =
                        sqlCreateTable.getAddedForeignKeys().stream().map(v -> Pair.of(v.refTableName, v))
                            .collect(Collectors.toList());

                    if (refTables.stream().allMatch(refTable ->
                        ExecutionStrategy.pushableForeignConstraint(plannerContext, dbName, tbName, refTable,
                            sqlCreateTable))) {
                        // Can push down.
                        sqlCreateTable.setPushDownForeignKeys(true);
                        for (ForeignKeyData data : sqlCreateTable.getAddedForeignKeys()) {
                            data.setPushDown(true);
                        }
//                        sqlCreateTable.removeForeignKeys();
                    }

                    // Remove referenced table replacement.
                    if (!sqlCreateTable.getPushDownForeignKeys()) {
                        sqlCreateTable.setLogicalReferencedTables(null);
                    }
                }

                return LogicalCreateTable.create((CreateTable) ddl);
            } else if (ddl instanceof AlterTable) {
                boolean isAlterLocalPartition = ddl.sqlNode instanceof SqlAlterTableRepartitionLocalPartition;
                boolean isRemoveLocalPartition = ddl.sqlNode instanceof SqlAlterTableRemoveLocalPartition;
                if (isAlterLocalPartition || isRemoveLocalPartition) {
                    return LogicalAlterTable.create((AlterTable) ddl);
                }
                SqlAlterTable sqlAlterTable = (SqlAlterTable) ddl.getSqlNode();
                SqlIdentifier tbNameId = (SqlIdentifier) sqlAlterTable.getName();
                Pair<String, String> dbAndTb = CalciteUtils.getDbNameAndTableNameByTableIdentifier(tbNameId);
                String dbName = dbAndTb.getKey();
                String tbName = dbAndTb.getValue();
                if (sqlAlterTable.getAlters().size() == 1) {
                    if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableSplitPartitionByHotValue) {
                        return LogicalAlterTableSplitPartitionByHotValue.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableExtractPartition) {
                        return LogicalAlterTableExtractPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableSplitPartition) {
                        return LogicalAlterTableSplitPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableExpandPartitions) {
                        return LogicalAlterTableExpandPartitions.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableCancelExpand) {
                        return LogicalAlterTableCancelExpand.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableMergePartition) {
                        return LogicalAlterTableMergePartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableMovePartition) {
                        return LogicalAlterTableMovePartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableAddPartition) {
                        return LogicalAlterTableAddPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableDropPartition) {
                        return LogicalAlterTableDropPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableTruncatePartition) {
                        return LogicalAlterTableTruncatePartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableOptimizePartition) {
                        return LogicalAlterTableOptimizePartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableReorgPartition) {
                        return LogicalAlterTableReorgPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableModifyPartitionValues) {
                        return LogicalAlterTableModifyPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableModifySubPartitionValues) {
                        return LogicalAlterTableModifyPartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableRenamePartition) {
                        return LogicalAlterTableRenamePartition.create(ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAddForeignKey) {
                        // Check and set FK before all with EC context.
                        ForeignKeyUtils.checkSetForeignKey(sqlAlterTable, plannerContext, tbName);
                        return LogicalAlterTable.create((AlterTable) ddl);
                    } else if (sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableExchangePartition) {
                        AlterTable alterTable = (AlterTable) ddl;
                        SqlAlterTableExchangePartition sqlAlterTableExchangePartition =
                            (SqlAlterTableExchangePartition) sqlAlterTable.getAlters().get(0);

                        String targetSchema = PlannerContext.getPlannerContext(ddl).getSchemaName();
                        String srcSchema = PlannerContext.getPlannerContext(ddl).getSchemaName();
                        SqlIdentifier srcTableNameNode = ((SqlIdentifier) alterTable.getTableName());
                        SqlIdentifier tarTableNameNode =
                            ((SqlIdentifier) sqlAlterTableExchangePartition.getTableName());
                        String srcTableName = targetSchema;
                        String tarTableName = targetSchema;
                        if (srcTableNameNode.isSimple()) {
                            srcTableName = srcTableNameNode.getSimple();
                        } else {
                            srcSchema = srcTableNameNode.names.get(0);
                            srcTableName = srcTableNameNode.getLastName();
                        }
                        if (tarTableNameNode.isSimple()) {
                            tarTableName = tarTableNameNode.getSimple();
                        } else {
                            targetSchema = tarTableNameNode.names.get(0);
                            tarTableName = tarTableNameNode.getLastName();
                        }
                        final TableMeta srcMeta =
                            this.plannerContext.getExecutionContext().getSchemaManager(srcSchema)
                                .getTable(srcTableName);
                        final Engine srcTbEngine = srcMeta.getEngine();
                        final TableMeta tarMeta =
                            this.plannerContext.getExecutionContext().getSchemaManager(targetSchema)
                                .getTable(tarTableName);
                        final Engine tarTbEngine = tarMeta.getEngine();
                        if (Engine.isFileStore(srcTbEngine) || Engine.isFileStore(tarTbEngine)) {
                            return LogicalAlterTable.create((AlterTable) ddl);
                        } else {
                            if (!srcSchema.equalsIgnoreCase(targetSchema)) {
                                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                    "It is not allowed to execute exchange partition command cross database");
                            }
                            return LogicalAlterTableExchangePartition.create(ddl);
                        }
                    } else {
                        return LogicalAlterTable.create((AlterTable) ddl);
                    }
                } else {
                    return LogicalAlterTable.create((AlterTable) ddl);
                }

            } else if (ddl instanceof RenameTables) {
                return LogicalRenameTables.create((RenameTables) ddl);
            } else if (ddl instanceof RenameTable) {
                return LogicalRenameTable.create((RenameTable) ddl);
            } else if (ddl instanceof TruncateTable) {
                if (((TruncateTable) ddl).isInsertOverwriteSql()) {
                    return LogicalInsertOverwrite.create((TruncateTable) ddl);
                } else {
                    return LogicalTruncateTable.create((TruncateTable) ddl);
                }
            } else if (ddl instanceof DropTable) {
                return LogicalDropTable.create((DropTable) ddl);

            } else if (ddl instanceof DropMaterializedView) {
                return convertDropMaterializedView((DropMaterializedView) ddl);
            } else if (ddl instanceof CreateIndex) {
                return LogicalCreateIndex.create((CreateIndex) ddl);

            } else if (ddl instanceof DropIndex) {
                return LogicalDropIndex.create((DropIndex) ddl);

            } else if (ddl instanceof CreateIndexInDatabase) {
                return LogicalCreateIndexInDatabase.create((CreateIndexInDatabase) ddl);

            } else if (ddl instanceof DropIndexInDatabase) {
                return LogicalDropIndexInDatabase.create((DropIndexInDatabase) ddl);

            } else if (ddl instanceof AlterRule) {
                return LogicalAlterRule.create((AlterRule) ddl);

            } else if (ddl.getSqlNode() instanceof SqlCheckGlobalIndex) {
                return LogicalCheckGsi.create((GenericDdl) ddl, (SqlCheckGlobalIndex) ddl.getSqlNode());

            } else if (ddl.getSqlNode() instanceof SqlCheckColumnarIndex) {
                return LogicalCheckCci.create((GenericDdl) ddl, (SqlCheckColumnarIndex) ddl.getSqlNode());

            } else if (ddl instanceof GenericDdl) {
                return LogicalGenericDdl.create((GenericDdl) ddl);

            } else if (ddl instanceof AlterTableGroupSplitPartition) {
                return LogicalAlterTableGroupSplitPartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupMergePartition) {
                return LogicalAlterTableGroupMergePartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupMovePartition) {
                return LogicalAlterTableGroupMovePartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupExtractPartition) {
                return LogicalAlterTableGroupExtractPartition.create(ddl);

            } else if (ddl instanceof AlterTableSetTableGroup) {
                return LogicalAlterTableSetTableGroup.create(ddl);

            } else if (ddl instanceof AlterTableGroupRenamePartition) {
                return LogicalAlterTableGroupRenamePartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupSetLocality) {
                return LogicalAlterTableGroupSetLocality.create(ddl);

            } else if (ddl instanceof AlterTableGroupSetPartitionsLocality) {
                return LogicalAlterTableGroupSetPartitionsLocality.create(ddl);

            } else if (ddl instanceof RefreshTopology) {
                return LogicalRefreshTopology.create(ddl);

            } else if (ddl instanceof AlterTableGroupAddPartition) {
                return LogicalAlterTableGroupAddPartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupDropPartition) {
                return LogicalAlterTableGroupDropPartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupTruncatePartition) {
                return LogicalAlterTableGroupTruncatePartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupOptimizePartition) {
                return LogicalAlterTableGroupOptimizePartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupReorgPartition) {
                return LogicalAlterTableGroupReorgPartition.create(ddl);

            } else if (ddl instanceof AlterTableGroupModifyPartition) {
                return LogicalAlterTableGroupModifyPartition.create(ddl);

            } else if (ddl instanceof MoveDatabase) {
                return LogicalMoveDatabases.create(ddl);

            } else if (ddl instanceof AlterTablePartitionCount) {
                return LogicalAlterTablePartitionCount.create((AlterTablePartitionCount) ddl);

            } else if (ddl instanceof AlterTableRemovePartitioning) {
                return LogicalAlterTableRemovePartitioning.create((AlterTableRemovePartitioning) ddl);

            } else if (ddl instanceof AlterTableRepartition) {
                return LogicalAlterTableRepartition.create((AlterTableRepartition) ddl);

            } else if (ddl instanceof AlterTableArchivePartition) {
                return LogicalAlterTableArchivePartition.create((AlterTableArchivePartition) ddl);

            } else if (ddl instanceof AlterTableRemoveAutoPartition) {
                return LogicalAlterTableRemoveAutoPartition.create((AlterTableRemoveAutoPartition) ddl);
            } else if (ddl instanceof AlterTableGroupSplitPartitionByHotValue) {
                return LogicalAlterTableGroupSplitPartitionByHotValue.create(ddl);

            } else if (ddl instanceof CreateJoinGroup) {
                return LogicalCreateJoinGroup.create((CreateJoinGroup) ddl);

            } else if (ddl instanceof DropJoinGroup) {
                return LogicalDropJoinGroup.create(ddl);

            } else if (ddl instanceof AlterJoinGroup) {
                return LogicalAlterJoinGroup.create(ddl);

            } else if (ddl instanceof MergeTableGroup) {
                return LogicalMergeTableGroup.create(ddl);

            } else if (ddl instanceof AlterTableGroupAddTable) {
                return LogicalAlterTableGroupAddTable.create(ddl);

            } else if (ddl instanceof AlterFileStorageAsOfTimestamp
                || ddl instanceof AlterFileStoragePurgeBeforeTimestamp
                || ddl instanceof AlterFileStorageBackup) {
                return LogicalAlterFileStorage.create(ddl);

            } else if (ddl instanceof DropFileStorage) {
                return LogicalDropFileStorage.create(ddl);
            } else if (ddl instanceof ClearFileStorage) {
                return LogicalClearFileStorage.create(ddl);
            } else if (ddl instanceof CreateFileStorage) {
                return LogicalCreateFileStorage.create(ddl);
            } else if (ddl instanceof CreateExternalCatalog) {
                CreateExternalCatalog extDdl =
                    (CreateExternalCatalog) ddl;
                return LogicalExternalCatalogDdl.create(ddl, DdlType.CREATE_EXTERNAL_CATALOG,
                    extDdl.getCatalogName(), extDdl.isIfNotExists(), false,
                    extDdl.getConnector(), extDdl.getProperties(),
                    extDdl.getSecretName(), extDdl.getComment(), null, null);
            } else if (ddl instanceof DropExternalCatalog) {
                DropExternalCatalog extDdl =
                    (DropExternalCatalog) ddl;
                return LogicalExternalCatalogDdl.create(ddl, DdlType.DROP_EXTERNAL_CATALOG,
                    extDdl.getCatalogName(), false, extDdl.isIfExists(),
                    null, null, null, null, null, null);
            } else if (ddl instanceof AlterExternalCatalog) {
                AlterExternalCatalog extDdl =
                    (AlterExternalCatalog) ddl;
                return LogicalExternalCatalogDdl.create(ddl, DdlType.ALTER_EXTERNAL_CATALOG,
                    extDdl.getCatalogName(), false, false,
                    null, extDdl.getProperties(), null,
                    extDdl.getComment(), null, null);
            } else if (ddl instanceof CreateSecret) {
                CreateSecret secretDdl =
                    (CreateSecret) ddl;
                return LogicalSecretDdl.create(ddl, DdlType.CREATE_SECRET,
                    secretDdl.getSecretName(), secretDdl.isIfNotExists(), false,
                    secretDdl.getProperties());
            } else if (ddl instanceof DropSecret) {
                DropSecret secretDdl =
                    (DropSecret) ddl;
                return LogicalSecretDdl.create(ddl, DdlType.DROP_SECRET,
                    secretDdl.getSecretName(), false, secretDdl.isIfExists(),
                    null);
            } else if (ddl instanceof AlterSecret) {
                AlterSecret secretDdl =
                    (AlterSecret) ddl;
                return LogicalSecretDdl.create(ddl, DdlType.ALTER_SECRET,
                    secretDdl.getSecretName(), false, false,
                    secretDdl.getProperties());
            } else if (ddl instanceof CreateStoragePool) {
                return LogicalCreateStoragePool.create(ddl);
            } else if (ddl instanceof AlterStoragePool) {
                return LogicalAlterStoragePool.create(ddl);
            } else if (ddl instanceof DropStoragePool) {
                return LogicalDropStoragePool.create(ddl);
            } else if (ddl instanceof OptimizeTable) {
                return LogicalOptimizeTable.create((OptimizeTable) ddl);
            } else if (ddl instanceof AnalyzeTable) {
                return LogicalAnalyzeTable.create((AnalyzeTable) ddl);
            } else if (ddl instanceof PushDownUdf) {
                return LogicalPushDownUdf.create((PushDownUdf) ddl);
            } else if (ddl instanceof CreateView) {
                return LogicalCreateView.create((CreateView) ddl);
            } else if (ddl instanceof DropView) {
                return LogicalDropView.create((DropView) ddl);
            } else if (ddl instanceof CreateFunction) {
                return LogicalCreateFunction.create((CreateFunction) ddl);

            } else if (ddl instanceof DropFunction) {
                return LogicalDropFunction.create((DropFunction) ddl);

            } else if (ddl instanceof CreateJavaFunction) {
                return LogicalCreateJavaFunction.create((CreateJavaFunction) ddl);
            } else if (ddl instanceof DropJavaFunction) {
                return LogicalDropJavaFunction.create((DropJavaFunction) ddl);
            } else if (ddl instanceof CreateProcedure) {
                return LogicalCreateProcedure.create((CreateProcedure) ddl);

            } else if (ddl instanceof DropProcedure) {
                return LogicalDropProcedure.create((DropProcedure) ddl);
            } else if (ddl instanceof AlterProcedure) {
                return LogicalAlterProcedure.create((AlterProcedure) ddl);
            } else if (ddl instanceof AlterFunction) {
                return LogicalAlterFunction.create((AlterFunction) ddl);
            } else if (ddl instanceof AlterDatabase) {
                return LogicalAlterDatabase.create((AlterDatabase) ddl);
            } else if (ddl instanceof ImportDatabase) {
                return LogicalImportDatabase.create((ImportDatabase) ddl);
            } else if (ddl instanceof ImportSequence) {
                return LogicalImportSequence.create((ImportSequence) ddl);
            } else if (ddl instanceof AlterInstance) {
                return LogicalAlterInstance.create((AlterInstance) ddl);
            } else if (ddl instanceof AlterTableToggleFullScan) {
                return LogicalAlterTableToggleFullScan.create((AlterTableToggleFullScan) ddl);
            } else if (ddl instanceof AlterTableGhost) {
                return LogicalAlterTableGhost.create(ddl);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED,
                    "operation " + ddl.getSqlNode().getKind());
            }
        } else {
            // The plan will be executed separately (not via DDL Engine).
            if (ddl instanceof CreateDatabase) {
                return LogicalCreateDatabase.create((CreateDatabase) ddl);

            } else if (ddl instanceof DropDatabase) {
                return LogicalDropDatabase.create((DropDatabase) ddl);
            }

            if (ddl instanceof CreateMaterializedView) {
                return convertCreateMaterializedView((CreateMaterializedView) ddl);
            } else if (ddl.getSqlNode() instanceof SqlRebalance) {
                return LogicalRebalance.create((GenericDdl) ddl, (SqlRebalance) ddl.getSqlNode());
            } else if (ddl.getSqlNode() instanceof SqlCheckGlobalIndex) {
                return LogicalCheckGsi.create((GenericDdl) ddl, (SqlCheckGlobalIndex) ddl.getSqlNode());
            } else if (ddl.getSqlNode() instanceof SqlCheckColumnarIndex) {
                return LogicalCheckCci.create((GenericDdl) ddl, (SqlCheckColumnarIndex) ddl.getSqlNode());
            } else if (ddl instanceof ChangeConsensusRole) {
                return LogicalChangeConsensusLeader.create((ChangeConsensusRole) ddl);
            } else if (ddl instanceof AlterSystemSetConfig) {
                return LogicalAlterSystemSetConfig.create((AlterSystemSetConfig) ddl);
            } else if (ddl instanceof CreateTableGroup) {
                return LogicalCreateTableGroup.create((CreateTableGroup) ddl);
            } else if (ddl instanceof DropTableGroup) {
                return LogicalDropTableGroup.create((DropTableGroup) ddl);
            } else if (ddl instanceof UnArchive) {
                return LogicalUnArchive.create(ddl);
            } else if (ddl instanceof InspectIndex) {
                return LogicalInspectIndex.create((InspectIndex) ddl);
            } else if (ddl instanceof CreateJoinGroup) {
                return LogicalCreateJoinGroup.create((CreateJoinGroup) ddl);
            } else if (ddl instanceof DropJoinGroup) {
                return LogicalDropJoinGroup.create(ddl);
            } else if (ddl instanceof ConvertAllSequences) {
                return LogicalConvertAllSequences.create((ConvertAllSequences) ddl);
            }

            if (ddl instanceof SequenceDdl) {
                return LogicalSequenceDdl.create((SequenceDdl) ddl);
            }

            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED,
                "operation " + ddl.getSqlNode().getKind());
        }
    }

    private boolean isSupportedByNewDdlEngine(DDL ddl) {
        return ddl.kind().belongsTo(SqlKind.DDL_SUPPORTED_BY_NEW_ENGINE);
    }

    private RelNode convertCreateMaterializedView(final CreateMaterializedView ddl) {
        CreateMaterializedView newView = (CreateMaterializedView) super.visit(ddl);
        return LogicalCreateMaterializedView.createMaterializedView(
            newView, newView.getTraitSet().replace(DrdsConvention.NONE), ddl.bRefresh);
    }

    private RelNode convertDropMaterializedView(final DropMaterializedView ddl) {
        return new LogicalDropMaterializedView(ddl.getCluster(), ddl.getSchemaName(), ddl.getViewName(), true);
    }

    private void updateTableProperties(Map<String, TableProperties> tablePropertiesMap, RelNode scanOrLookup) {

        this.allTableSingle = (allTableSingle && tablePropertiesMap.values().stream().allMatch(
            t -> t.isSingleTable()));
        final boolean allTableInOneGroup = RelUtils.allTableInOneGroup(tablePropertiesMap);
        if (allTableSingleWithSameGroup) {
            if (!allTableInOneGroup) {
                allTableSingleWithSameGroup = false;
            }
            if (scanOrLookup != null) {
                if (scanOrLookup instanceof LogicalView) {
                    if (!((LogicalView) scanOrLookup).isSingleGroup()) {
                        allTableSingleWithSameGroup = false;
                    }
                }

                if (allTableSingleWithSameGroup && scanOrLookup instanceof TableScan) {
                    TableScan tblScan = (TableScan) scanOrLookup;
                    final List<String> qualifiedName = tblScan.getTable().getQualifiedName();
                    final String tbName = Util.last(qualifiedName);
                    // final String dbName = qualifiedName.size() == 2 ? qualifiedName.get(0) : null;

                    final TableProperties tblProps = tablePropertiesMap.get(tbName);
                    if (tblProps != null && tblProps.getPartInfo() != null) {
                        updateAllTableSingleWithSameTgFlag(tblProps.getPartInfo());
                    }
                }
            }
        }

        final boolean allTableBroadcast = RelUtils.allTableBroadcast(tablePropertiesMap);
        if (this.allTableBroadcast && !allTableBroadcast) {
            this.allTableBroadcast = false;
        }

        final boolean allTableNotBroadcast = RelUtils.allTableNotBroadcast(tablePropertiesMap);
        if (this.allTableSingleNoBroadcast && !(allTableSingleWithSameGroup && allTableNotBroadcast)) {
            this.allTableSingleNoBroadcast = false;
        }
        for (Map.Entry<String, TableProperties> entry : tablePropertiesMap.entrySet()) {
            if (Engine.isFileStore(entry.getValue().getEngine())) {
                existsOSSTable = true;
            }
        }
    }

    /**
     * Check and update flag {@link #allTableSingleWithSameGroup}.
     * Initialize {@link #allTableSingleTgId} at first call
     *
     * @param tblPartInfo partition info of single table
     * @return updated allTableSingle flag value
     */
    private boolean updateAllTableSingleWithSameTgFlag(PartitionInfo tblPartInfo) {
        if (allTableSingleWithSameGroup
            && null != tblPartInfo
            && DbInfoManager.getInstance().isNewPartitionDb(tblPartInfo.getTableSchema())) {
            if (tblPartInfo.isGsiSingleOrSingleTable()) {
                Long tgId = tblPartInfo.getTableGroupId();
                if (allTableSingleTgId == null) {
                    allTableSingleTgId = tgId;
                } else {
                    /**
                     * For autodb, only the single tables in the same tablegroup are allowed to
                     * push down join
                     */
                    if (!allTableSingleTgId.equals(tgId)) {
                        allTableSingleWithSameGroup = false;
                    }
                }
            } else if (tblPartInfo.isGsiOrPartitionedTable()) {
                allTableSingleWithSameGroup = false;
            }
        }

        return allTableSingleWithSameGroup;
    }

    private RelNode handleOptimizeTable(Dal dalNode) {
        final SqlOptimizeTable optimizeTable = (SqlOptimizeTable) dalNode.getAst();
        final String defaultSchemaName = PlannerContext.getPlannerContext(dalNode).getSchemaName();

        final Map<String, List<List<String>>> targetTable = new LinkedHashMap<>();
        final List<String> tableNames = new LinkedList<>();

        /* 获得所有逻辑表 */
        for (SqlNode tableNameNode : optimizeTable.getTableNames()) {
            String tableName = RelUtils.lastStringValue(tableNameNode);
            if (tableNames.contains(tableName)) {
                continue;
            }
            tableNames.add(tableName);

            PartitionInfoUtil.getTableTopology(defaultSchemaName, tableName).forEach((db, tables) -> {
                targetTable.computeIfAbsent(db, (x) -> new ArrayList<>())
                    .addAll(tables);
            });
        }

        int tableCount = PlannerUtils.tableCount(targetTable);

        if (tableCount == 0) {
            throw new IllegalArgumentException("Can't find proper actual target!");
        }

        SqlOptimizeTable newSqlNode = new SqlOptimizeTable(optimizeTable.getParserPosition(),
            ImmutableList.of(optimizeTable.getTableName()),
            optimizeTable.isNoWriteToBinlog(),
            optimizeTable.isLocal());
        String schemaName = newSqlNode.getDbName() != null ? newSqlNode.getDbName().toString() : null;
        PhyDal phyDal = new PhyDal(dalNode.getCluster(),
            dalNode.getTraitSet(),
            newSqlNode,
            dalNode.getRowType(),
            targetTable,
            tableNames,
            schemaName);
        if (tableCount == 1) {
            return phyDal;
        }

        return Gather.create(phyDal);
    }

    public LogicalView getBaseLogicalView() {
        if (tableNames.size() > 0) {
            baseLogicalView.setTableName(tableNames);
        }
        return baseLogicalView;
    }

    public boolean isDirectInTheSameDB() {
        return allTableSingleWithSameGroup && baseLogicalView != null && schemaNames.size() <= 1;
    }

    public boolean isDirectInDifferentDB() {
        boolean ret = allTableSingle && baseLogicalView != null;
        if (ret) {
            String lastStorageId = null;
            for (Map<Long, String> storages : tableStorages) {
                if (storages == null || storages.isEmpty()) {
                    ret = false;
                    return ret;
                }
                for (String id : storages.values()) {
                    if (lastStorageId == null) {
                        lastStorageId = id;
                    } else {
                        if (!lastStorageId.equalsIgnoreCase(id)) {
                            ret = false;
                            return ret;
                        }
                    }
                }
            }
        }
        return ret;
    }

    public List<String> getSchemaNames() {
        return schemaNames;
    }

    @VisibleForTesting
    public boolean isAllTableSingle() {
        return allTableSingle;
    }

    public boolean isAllTableBroadcast() {
        return allTableSingleWithSameGroup && allTableBroadcast;
    }

    public boolean isAllTableSingleNoBroadcast() {
        return allTableSingleNoBroadcast;
    }

    public boolean isShouldRemoveSchemaName() {
        return shouldRemoveSchemaName;
    }

    /**
     * TODO: 需要考虑 information_schema 等这些比较特殊的系统库
     */
    public void setShouldRemoveSchemaName(List<String> qualifiedName) {
        if (!shouldRemoveSchemaName && qualifiedName.size() == 2) {

            String dbName = qualifiedName.get(0);

            if (systemDbName.contains(dbName)) {
                shouldRemoveSchemaName = false;
            } else {
                shouldRemoveSchemaName = true;

            }
        }

    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public boolean isWithIndexHint() {
        return withIndexHint;
    }

    public boolean isModifyBroadcastTable() {
        return modifyBroadcastTable;
    }

    public boolean isOnlyBroadcastTable() {
        return allTableBroadcast;
    }

    public boolean isModifyGsiTable() {
        return modifyGsiTable;
    }

    public boolean isModifyForeignKey() {
        return modifyForeignKey;
    }

    public boolean isModifyExternalizedData() {
        return modifyExternalizedData;
    }

    private void detectExternalizedUpdate(LogicalModify logicalModify,
                                          Map<String, TableProperties> targetTableProperties) {
        if (!logicalModify.isUpdate() || logicalModify.getUpdateColumnList() == null
            || logicalModify.getTargetTableNames() == null) {
            return;
        }
        final int assignmentCount = Math.min(logicalModify.getUpdateColumnList().size(),
            logicalModify.getTargetTableNames().size());
        for (int i = 0; i < assignmentCount; i++) {
            final String targetTableName = logicalModify.getTargetTableNames().get(i);
            TableProperties properties = targetTableProperties.get(targetTableName);
            if (properties == null) {
                for (TableProperties candidate : targetTableProperties.values()) {
                    if (candidate.getTableName().equalsIgnoreCase(targetTableName)) {
                        properties = candidate;
                        break;
                    }
                }
            }
            if (properties == null) {
                continue;
            }
            final TableMeta tableMeta = plannerContext.getExecutionContext()
                .getSchemaManager(properties.getSchemaName()).getTableWithNull(properties.getTableName());
            if (tableMeta == null) {
                continue;
            }
            final String updateColumn = logicalModify.getUpdateColumnList().get(i);
            if (tableMeta.getColumnMceState(updateColumn).isRenameToAddr()
                || tableMeta.getColumnMceState(updateColumn).isDualWrite()) {
                modifyExternalizedData = true;
                return;
            }
        }
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public static class ReplaceTableScanInFilterSubQueryFinder extends RexShuttle {

        private final PlannerContext plannerContext;
        // Whether all tables are broadcast
        private boolean allTableBroadcast = true;
        // Whether all tables are single and in the same group and no broadcast table
        private boolean allTableSingleNoBroadcast = true;
        private boolean allTableSingle = true;
        private boolean allTableSingleWithSameGroup = true;
        private boolean containUncertainValue = false;
        private boolean containComplexExpression = false;
        private boolean existsNonPushDownFunc = false;
        private String singleDbIndex = null;
        private LogicalView baseLogicalView = null;
        private List<String> tableNames = new ArrayList<>();
        private List<Map<Long, String>> storageIds = new ArrayList<>();
        private SqlKind sqlKind;
        private LockMode lockMode = LockMode.UNDEF;
        private List<String> schemaNames;
        private boolean existsOSSTable;
        private boolean existPagingForce = false;
        private boolean existForceColumnar = false;
        private boolean allTableHaveColumnar = true;
        private boolean existsWindow = false;
        private Long allTableSingleTgId = null;
        /**
         * The groupInfo of all replicas tables in plan
         */
        private DirectPlanCommonGroupInfo planCommonGroupInfo = new DirectPlanCommonGroupInfo();

        public ReplaceTableScanInFilterSubQueryFinder(SqlKind kind, LockMode lockMode, boolean allTableSingle,
                                                      boolean allTableBroadcast, boolean allTableSingleNoBroadcast,
                                                      String singleDbIndex, List<String> schemaNames,
                                                      PlannerContext pc) {

            this.sqlKind = kind;
            this.lockMode = lockMode;
            this.allTableBroadcast = allTableBroadcast;
            this.allTableSingleNoBroadcast = allTableSingleNoBroadcast;
            this.allTableSingleWithSameGroup = allTableSingleWithSameGroup;
            this.singleDbIndex = singleDbIndex;
            this.schemaNames = schemaNames;
            this.plannerContext = pc;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            /**
             * Do not support UPDATE and DELETE with subQuery
             */
            containComplexExpression = true;
            if (sqlKind == SqlKind.UPDATE || sqlKind == SqlKind.DELETE) {
                if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                    throw new TddlRuntimeException(ERR_DML_WITH_SUBQUERY);
                }
            }

            ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
            visitor.plannerContext = plannerContext;
            visitor.lockMode = lockMode;
            visitor.allTableSingleWithSameGroup = allTableSingleWithSameGroup;
            visitor.allTableSingle = allTableSingle;
            visitor.singleDbIndex = singleDbIndex;
            visitor.schemaNames = this.schemaNames;
            visitor.allTableBroadcast = allTableBroadcast;
            visitor.allTableSingleNoBroadcast = allTableSingleNoBroadcast;
            visitor.tableStorages = storageIds;

            MergedStorageInfo mergedStorageInfo =
                plannerContext.getExecutionContext().getStorageInfo(plannerContext.getSchemaName());
            visitor.mysql80 = (mergedStorageInfo != null) && mergedStorageInfo.isMysql80();
            RelNode r = subQuery.rel.accept(visitor);
            this.allTableSingleWithSameGroup = visitor.allTableSingleWithSameGroup;
            this.allTableSingle = visitor.allTableSingle;
            this.singleDbIndex = visitor.singleDbIndex;
            this.baseLogicalView = visitor.baseLogicalView;
            this.tableNames = visitor.tableNames;
            this.storageIds = visitor.tableStorages;
            this.allTableBroadcast = visitor.allTableBroadcast;
            this.allTableSingleNoBroadcast = visitor.allTableSingleNoBroadcast;
            this.existsOSSTable = visitor.existsOSSTable;
            this.existPagingForce |= visitor.existPagingForce;
            this.existForceColumnar |= visitor.existForceColumnar;
            this.allTableHaveColumnar &= visitor.allTableHaveColumnar;
            this.allTableSingleTgId = visitor.allTableSingleTgId;
            this.existsNonPushDownFunc |= visitor.existsNonPushDownFunc;
            this.existsWindow |= visitor.existsWindow;
            this.planCommonGroupInfo = visitor.commonGroupKeyInfo;
            return subQuery.clone(r);
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            checkUncertainValue(call);
            return super.visitCall(call);
        }

        private void checkUncertainValue(final RexCall call) {
            if (containUncertainValue) {
                return;
            } else {
                final SqlOperator operator = call.getOperator();
                if (operator.isDynamicFunction()) {
                    containUncertainValue = true;
                }
            }
        }

        /**
         * all tables are single, and they in the same table group.
         */
        public boolean isAllSingleTableWithSameGroup() {
            return allTableSingleWithSameGroup;
        }

        /**
         * all tables are single although they maybe from different table group and schema.
         */
        public boolean isAllSingleTable() {
            return allTableSingle;
        }

        public boolean existsOSSTable() {
            return existsOSSTable;
        }

        public boolean isExistPagingForce() {
            return existPagingForce;
        }

        public boolean isExistForceColumnar() {
            return existForceColumnar;
        }

        public boolean isAllTableHaveColumnar() {
            return allTableHaveColumnar;
        }

        public boolean isAllTableBroadcast() {
            return allTableBroadcast;
        }

        public boolean isAllTableSingleNoBroadcast() {
            return allTableSingleNoBroadcast;
        }

        public String getSingleDbIndex() {
            return singleDbIndex;
        }

        public LogicalView getBaseLogicalView() {
            return baseLogicalView;
        }

        public List<String> getTableNames() {
            return tableNames;
        }

        public boolean isContainUncertainValue() {
            return containUncertainValue;
        }

        public boolean isContainComplexExpression() {
            return containComplexExpression;
        }

        public boolean isExistsNonPushDownFunc() {
            return existsNonPushDownFunc;
        }

        public boolean isExistsWindow() {
            return existsWindow;
        }

        public Long getAllTableSingleTgId() {
            return allTableSingleTgId;
        }

        public DirectPlanCommonGroupInfo getPlanCommonGroupInfo() {
            return planCommonGroupInfo;
        }
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    public List<TableProperties> getModifiedTables() {
        return modifiedTables;
    }

    public boolean isModifyShardingColumn() {
        return modifyShardingColumn;
    }

    public boolean isContainUncertainValue() {
        return containUncertainValue;
    }

    public boolean isContainComplexExpression() {
        return containComplexExpression;
    }

    public boolean isContainScaleOutWritableTable() {
        return containScaleOutWritableTable;
    }

    public boolean isContainReplicateWriableTable() {
        return containReplicateWriableTable;
    }

    /**
     * (modifyBroadcastTable && containUncertainValue) 条件：
     * DML 语句情况下，广播表如果包含不确定值，不能下推执行，例如：
     * update/delete from t where id > rand();
     * update/delete from t set time = current_time() order by rand();
     * <p>
     * 某些特殊情况似乎又能下推：(没有很好的办法识别，暂时先禁止下推)
     * update/delete where 2 > rand()
     */
    public boolean existsCannotPushDown() {
        return existsIntersect ||
            existsMinus || existsCheckSum || existsUnpushableAgg || existsNonPushDownFunc ||
            (modifyBroadcastTable && containUncertainValue) || existsCheckSumV2 ||
            existsUnPushedDynamicValues || insertSelectWithFlashback || existPagingForce
            || isExistsJoinWithBroadcastTblWithLocality() || notAllowDirectPushDownWithReplicasTables();
    }

    public boolean isContainOnlineModifyColumnTable() {
        return containOnlineModifyColumnTable;
    }

    public boolean isContainGeneratedColumn() {
        return containGeneratedColumn;
    }

    public boolean isExistsGroupingSets() {
        return existsGroupingSets;
    }

    public boolean isModifyWithLimitOffset() {
        return modifyWithLimitOffset;
    }

    public boolean existsOSSTable() {
        return existsOSSTable;
    }

    public boolean isExistPagingForce() {
        return existPagingForce;
    }

    public boolean isExistForceColumnar() {
        return existForceColumnar;
    }

    public boolean isAllTableHaveColumnar() {
        return allTableHaveColumnar;
    }

    public boolean isExistsCheckSum() {
        return existsCheckSum;
    }

    public boolean isExistsUnpushableAgg() {
        return existsUnpushableAgg;
    }

    public boolean isExistsCheckSumV2() {
        return existsCheckSumV2;
    }

    public boolean isOutFileStatistics() {
        return outFileStatistics;
    }

    public boolean isExistsBroadcastTblWithLocality() {
        return existsBroadcastTblWithLocality;
    }

    public boolean isExistsJoinWithBroadcastTblWithLocality() {
        return isExistsBroadcastTblWithLocality() && tableNames.size() > 1;
    }

    public boolean isExistsJoinWithReplicasTables() {
        return this.commonGroupKeyInfo.isContainAnyReplicasTables() && tableNames.size() > 1;
    }

    public boolean notAllowDirectPushDownWithReplicasTables() {

        if (!commonGroupKeyInfo.isContainAnyReplicasTables()) {
            return false;
        }

        String schemaName = this.schemaNames.get(0);
        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return true;
        }

        if (this.schemaNames.size() > 1) {
            return true;
        }

        /**
         * When replicas table are more than one table,
         * it it not allowed to do direct push down in PostPlanner,
         * so it will skipPostPlanner
         */
        if (isExistsJoinWithReplicasTables()) {
            /**
             * if the groupKey of commonGroupKeyInfo is empty,
             * it means that the groupKey of curr logicalPlan is conflicted
             * and cannot be direct pushdown
             */
            if (!commonGroupKeyInfo.isContainAnyPartitionedTables()) {
                if (commonGroupKeyInfo.getCommonGroupKeySet().isEmpty()) {
                    return true;
                }
            }
        }

        return false;
    }

    public DirectPlanCommonGroupInfo getCommonGroupKeyInfo() {
        return commonGroupKeyInfo;
    }

    public TtlQueryType getTtlQueryType() {
        return ttlQueryType;
    }

    /**
     * 检查自增列是否可以跳过 PK 检查。
     * 使用 ColumnSourceShuttle 沿着逻辑计划树追踪列值来源。
     *
     * @param logicalInsert INSERT 节点
     * @param columnName 自增列名
     * @return true 表示可以跳过 PK 检查
     */
    private boolean checkAutoIncColumnCanSkipPkCheck(LogicalInsert logicalInsert, String columnName) {
        RelNode input = logicalInsert.getInput();
        if (input == null) {
            return false;
        }

        // 从 insertRowType 中查找列索引
        RelDataType insertRowType = logicalInsert.getInsertRowType();
        if (insertRowType == null) {
            return false;
        }

        int columnIndex = -1;
        List<String> fieldNames = insertRowType.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (StringUtils.equalsIgnoreCase(fieldNames.get(i), columnName)) {
                columnIndex = i;
                break;
            }
        }

        // 列未在 INSERT 列表中指定，将使用自增值
        if (columnIndex == -1) {
            return true;
        }

        // 使用 ColumnSourceShuttle 追踪列值来源
        RexUtils.ColumnSourceShuttle.ValueSource source =
            RexUtils.ColumnSourceShuttle.analyze(input, columnIndex);

        // 如果列值为 null 字面量，可以跳过 PK 检查
        return source == RexUtils.ColumnSourceShuttle.ValueSource.NULL_LITERAL;
    }

    /**
     * Resolves the physical partition to use for a SHOW command targeting a partitioned table.
     * When SHOW_COMMAND_RAND_DISPATCH is enabled, picks a random partition; otherwise picks the first.
     *
     * @param modeHolder single-element array; on return, modeHolder[0] is set to DB_INDEX_MODE_RANDOM
     * if random dispatch was applied, or left as DB_INDEX_MODE_NORMAL otherwise.
     */
    @VisibleForTesting
    PhysicalPartitionInfo resolvePartitionedTableDbIndex(SqlDal sqlDal,
                                                         PartitionInfoManager partInfoMgr,
                                                         String logicalTable,
                                                         int[] modeHolder) {
        if (isRandomShowKind(sqlDal)) {
            modeHolder[0] = LogicalShow.DB_INDEX_MODE_RANDOM;
            return partInfoMgr.getRandomPhysicalPartition(logicalTable);
        } else {
            modeHolder[0] = LogicalShow.DB_INDEX_MODE_NORMAL;
            return partInfoMgr.getFirstPhysicalPartition(logicalTable);
        }
    }

    /**
     * Resolves the DN group index for a SHOW command that has no table name (e.g. SHOW STATUS).
     * When SHOW_COMMAND_RAND_DISPATCH is enabled, picks a random DN from the schema's group list.
     *
     * @param defaultDbIndex the current default DB index to fall back to when random dispatch is off.
     * @param modeHolder single-element array; on return, modeHolder[0] is set to DB_INDEX_MODE_RANDOM
     * if random dispatch was applied, or unchanged otherwise.
     * @return the resolved DB index (may equal defaultDbIndex when random dispatch is off).
     */
    @VisibleForTesting
    String resolveNoTableNameDbIndex(SqlDal sqlDal, String schemaName, String defaultDbIndex, int[] modeHolder) {
        if (!isRandomShowKind(sqlDal)) {
            return defaultDbIndex;
        }
        modeHolder[0] = LogicalShow.DB_INDEX_MODE_RANDOM;
        List<Group> groups = OptimizerContext.getContext(schemaName).getMatrix().getGroups();
        int randomIndex = ThreadLocalRandom.current().nextInt(groups.size());
        return groups.get(randomIndex).getName();
    }

    private boolean isRandomShowKind(SqlDal sqlDal) {
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.SHOW_COMMAND_RAND_DISPATCH)) {
            return false;
        }
        if (sqlDal.getKind() != SqlKind.SHOW) {
            return false;
        }
        return true;
    }
}
