package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Builds precise physical-to-logical name mappings by querying CN metadata
 * (topology information), instead of guessing via suffix pattern matching.
 * <p>
 * This approach is reliable for all physical table name formats, including
 * random suffixes like {@code _8htn}, {@code _v3gg}, as well as numeric
 * suffixes like {@code _00001}.
 * <p>
 * Usage:
 * <pre>
 *   PhysicalToLogicalTableMapping mapping = PhysicalToLogicalTableMapping.buildForAllSchemas();
 *   String logicalTable = mapping.getLogicalTableName("web3_control_address_tag_model_category_pre_8htn");
 *   String logicalSchema = mapping.getLogicalSchemaName("mydb_p00001");
 * </pre>
 *
 * @see com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaTablesHandler#generateMetadataForSchema
 */
public class PhysicalToLogicalTableMapping {

    private static final Logger logger = LoggerFactory.getLogger(PhysicalToLogicalTableMapping.class);

    /**
     * physicalTableName (lower-case) → logicalTableName
     * Uses case-insensitive TreeMap for reliable lookups.
     */
    private final Map<String, String> physicalTableToLogical;

    /**
     * physicalDbName (lower-case) → logicalSchemaName
     * Uses case-insensitive TreeMap for reliable lookups.
     */
    private final Map<String, String> physicalDbToLogicalSchema;

    PhysicalToLogicalTableMapping(Map<String, String> physicalTableToLogical,
                                  Map<String, String> physicalDbToLogicalSchema) {
        this.physicalTableToLogical = physicalTableToLogical;
        this.physicalDbToLogicalSchema = physicalDbToLogicalSchema;
    }

    /**
     * Look up the logical table name for a given physical table name.
     * Falls back to {@link PhysicalNameExtractor} if no mapping is found.
     */
    public String getLogicalTableName(String physicalTableName) {
        if (physicalTableName == null) {
            return null;
        }
        String result = physicalTableToLogical.get(physicalTableName.toLowerCase());
        if (result != null) {
            return result;
        }
        return PhysicalNameExtractor.extractLogicalTableName(physicalTableName);
    }

    /**
     * Look up the logical schema name for a given physical DB name.
     * Falls back to {@link PhysicalNameExtractor} if no mapping is found.
     */
    public String getLogicalSchemaName(String physicalDbName) {
        if (physicalDbName == null) {
            return null;
        }
        String result = physicalDbToLogicalSchema.get(physicalDbName.toLowerCase());
        if (result != null) {
            return result;
        }
        return PhysicalNameExtractor.extractLogicalSchemaName(physicalDbName);
    }

    /**
     * Build mappings for all user schemas by scanning CN metadata.
     * <p>
     * Iterates over all user databases and collects topology information
     * from both DRDS mode ({@link TddlRule}) and Auto mode
     * ({@link PartitionInfoManager}).
     */
    public static PhysicalToLogicalTableMapping buildForAllSchemas() {
        Map<String, String> tableMapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> schemaMapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        try {
            List<String> allDatabases = DbInfoManager.getInstance().getDbList();
            List<String> userDatabases = new ArrayList<>();
            for (String db : allDatabases) {
                if (!SystemDbHelper.isDBBuildIn(db)) {
                    userDatabases.add(db);
                }
            }

            for (String schemaName : userDatabases) {
                try {
                    buildMappingsForSchema(schemaName, tableMapping, schemaMapping);
                } catch (Exception e) {
                    logger.warn("Failed to build physical-to-logical mapping for schema: " + schemaName, e);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to build physical-to-logical mappings", e);
        }

        logger.info("Built physical-to-logical mapping: " + tableMapping.size()
            + " table entries, " + schemaMapping.size() + " schema entries");

        return new PhysicalToLogicalTableMapping(tableMapping, schemaMapping);
    }

    /**
     * Build mappings for a single schema.
     */
    private static void buildMappingsForSchema(String schemaName,
                                               Map<String, String> tableMapping,
                                               Map<String, String> schemaMapping) {
        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        if (optimizerContext == null) {
            return;
        }
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        if (executorContext == null) {
            return;
        }

        TddlRuleManager ruleManager = optimizerContext.getRuleManager();
        PartitionInfoManager partitionInfoManager = optimizerContext.getPartitionInfoManager();
        ITopologyExecutor topologyExecutor = executorContext.getTopologyExecutor();

        // Collect all logical table names from both DRDS and Auto modes
        List<String> logicalTableNames = new ArrayList<>();

        // DRDS mode tables
        TddlRule tddlRule = ruleManager.getTddlRule();
        Collection<TableRule> tableRules = tddlRule.getTables();
        for (TableRule tableRule : tableRules) {
            String logicalTableName = tableRule.getVirtualTbName();
            if (!SystemTables.contains(logicalTableName)) {
                logicalTableNames.add(logicalTableName);
            }
        }

        // Auto mode tables
        for (PartitionInfo partitionInfo : partitionInfoManager.getPartitionInfos()) {
            String logicalTableName = partitionInfo.getTableName();
            if (!SystemTables.contains(logicalTableName)) {
                logicalTableNames.add(logicalTableName);
            }
        }

        // Build mappings for each logical table (including its GSI tables)
        SchemaManager schemaManager = optimizerContext.getLatestSchemaManager();
        for (String logicalTableName : logicalTableNames) {
            try {
                if (partitionInfoManager.isNewPartDbTable(logicalTableName)) {
                    buildAutoTableMapping(schemaName, logicalTableName, partitionInfoManager,
                        tableMapping, schemaMapping);
                } else {
                    buildDrdsTableMapping(schemaName, logicalTableName, ruleManager, topologyExecutor,
                        tableMapping, schemaMapping);
                }

                // Also build mappings for GSI tables associated with this logical table.
                // GSI tables have their own physical partitions that need to be mapped
                // so that LOGICAL_INDEX_USAGE can correctly identify GSI rows.
                buildGsiTableMappings(schemaName, logicalTableName, schemaManager,
                    partitionInfoManager, ruleManager, topologyExecutor, tableMapping, schemaMapping);
            } catch (Exception e) {
                logger.warn("Failed to build mapping for table: " + schemaName + "." + logicalTableName, e);
            }
        }
    }

    /**
     * Build mappings for all GSI tables associated with a logical table.
     * Each GSI has its own physical partitions that must be mapped to the GSI's logical name.
     */
    private static void buildGsiTableMappings(String schemaName,
                                              String logicalTableName,
                                              SchemaManager schemaManager,
                                              PartitionInfoManager partitionInfoManager,
                                              TddlRuleManager ruleManager,
                                              ITopologyExecutor topologyExecutor,
                                              Map<String, String> tableMapping,
                                              Map<String, String> schemaMapping) {
        try {
            TableMeta tableMeta = schemaManager.getTable(logicalTableName);
            if (tableMeta == null) {
                return;
            }
            Map<String, GsiMetaManager.GsiIndexMetaBean> publishedGsi = tableMeta.getGsiPublished();
            if (publishedGsi == null || publishedGsi.isEmpty()) {
                return;
            }
            for (String gsiName : publishedGsi.keySet()) {
                String gsiLower = gsiName.toLowerCase();
                try {
                    if (partitionInfoManager.isNewPartDbTable(gsiLower)) {
                        buildAutoTableMapping(schemaName, gsiLower, partitionInfoManager,
                            tableMapping, schemaMapping);
                    } else {
                        buildDrdsTableMapping(schemaName, gsiLower, ruleManager, topologyExecutor,
                            tableMapping, schemaMapping);
                    }
                } catch (Exception e) {
                    logger.warn("Failed to build mapping for GSI: " + schemaName + "." + gsiLower, e);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to build GSI mappings for table: " + schemaName + "." + logicalTableName, e);
        }
    }

    /**
     * Build mapping for an Auto-mode (new partition) table.
     * Uses {@link PartitionInfo} to get physical partition locations.
     */
    private static void buildAutoTableMapping(String schemaName,
                                              String logicalTableName,
                                              PartitionInfoManager partitionInfoManager,
                                              Map<String, String> tableMapping,
                                              Map<String, String> schemaMapping) {
        PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(logicalTableName);
        if (partitionInfo == null) {
            return;
        }
        List<PartitionSpec> partitions = partitionInfo.getPartitionBy().getPhysicalPartitions();
        for (PartitionSpec partition : partitions) {
            PartitionLocation location = partition.getLocation();
            String groupName = location.getGroupKey();
            String physicalDb = GroupInfoUtil.buildPhysicalDbNameFromGroupName(schemaName, groupName).toLowerCase();
            String physicalTable = location.getPhyTableName().toLowerCase();

            tableMapping.put(physicalTable, logicalTableName);
            schemaMapping.put(physicalDb, schemaName);
        }
    }

    /**
     * Build mapping for a DRDS-mode (sharding rule) table.
     * Uses {@link TableRule#getActualTopology()} to get physical table locations.
     */
    private static void buildDrdsTableMapping(String schemaName,
                                              String logicalTableName,
                                              TddlRuleManager ruleManager,
                                              ITopologyExecutor topologyExecutor,
                                              Map<String, String> tableMapping,
                                              Map<String, String> schemaMapping) {
        TableRule tableRule = ruleManager.getTableRule(logicalTableName);
        if (tableRule == null) {
            return;
        }

        Map<String, Set<String>> topology = tableRule.getStaticTopology();
        if (topology == null || topology.isEmpty()) {
            topology = tableRule.getActualTopology();
        }

        for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
            String groupName = entry.getKey();
            Set<String> physicalTableNames = entry.getValue();
            if (physicalTableNames == null || physicalTableNames.isEmpty()) {
                continue;
            }

            // Get physical DB name from group data source
            String physicalDb = getPhysicalDbFromGroup(groupName, topologyExecutor);
            if (physicalDb != null) {
                schemaMapping.put(physicalDb.toLowerCase(), schemaName);
            }

            for (String physicalTable : physicalTableNames) {
                tableMapping.put(physicalTable.toLowerCase(), logicalTableName);
            }
        }
    }

    /**
     * Resolve the physical DB name from a group name via the topology executor.
     */
    private static String getPhysicalDbFromGroup(String groupName, ITopologyExecutor topologyExecutor) {
        try {
            IGroupExecutor groupExecutor = topologyExecutor.getGroupExecutor(groupName);
            if (groupExecutor == null) {
                return null;
            }
            TGroupDataSource dataSource = (TGroupDataSource) groupExecutor.getDataSource();
            return dataSource.getConfigManager()
                .getDataSource(MasterSlave.MASTER_ONLY)
                .getDsConfHandle()
                .getRunTimeConf()
                .getDbName();
        } catch (Exception e) {
            logger.warn("Failed to get physical DB name for group: " + groupName, e);
            return null;
        }
    }
}
