package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GsiColsReplaceRule extends RelOptRule {

    public static final GsiColsReplaceRule INSTANCE = new GsiColsReplaceRule(BKAJoin.class);

    //~ Constructors -----------------------------------------------------------

    public GsiColsReplaceRule(
        Class<? extends Join> joinClass) {
        super(operand(joinClass, operand(LogicalIndexScan.class, none()), operand(LogicalView.class, none())),
            "GsiColsReplaceRule");
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!PlannerContext
            .getPlannerContext(call)
            .getParamManager()
            .getBoolean(ConnectionParams.ENABLE_GSI_LOOKUP_OPTIMIZE)) {
            return false;
        }

        // Column rearrangement breaks the FETCH_BLOB Project that
        // ToDrdsRelVisitor wraps LogicalView with FETCH_BLOB Project.
        // This rule runs in optimizeRowAfterCBO (HepPlanner), only during
        // plan generation — plan cache hit skips it entirely.
        LogicalIndexScan indexScan = call.rel(1);
        String mainTableName = indexScan.getMainTableName();
        if (mainTableName != null) {
            ExecutionContext ec = PlannerContext.getPlannerContext(call).getExecutionContext();
            if (TableColumnUtils.hasExternalizedColumn(ec, indexScan.getSchemaName(), mainTableName)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        BKAJoin join = call.rel(0);
        LogicalIndexScan indexScan = call.rel(1);
        LogicalView lv = call.rel(2);
        if (indexScan.isMGetEnabled() || indexScan.isProjectSwitched()) {
            return;
        }
        PlannerContext pc = PlannerContext.getPlannerContext(call);
        RelNode newRel = switchProject(call.getMetadataQuery(), pc, join, indexScan, lv);
        if (newRel == null) {
            return;
        }
        indexScan.setProjectSwitched(true);
        call.transformTo(newRel);
    }

    private RelNode switchProject(RelMetadataQuery mq, PlannerContext pc, BKAJoin join, LogicalIndexScan indexScan,
                                  LogicalView mainTableView) {
        if (indexScan == null || mainTableView == null) {
            return null;
        }
        if (!checkIndexScan(indexScan)) {
            return null;
        }

        String mainTableName = indexScan.getMainTableName();
        if (mainTableName == null) {
            return null;
        }
        if (!mainTableView.getTableNames().contains(mainTableName)) {
            return null;
        }

        Set<String> switchCols = new LinkedHashSet<>();
        for (int i = 0; i < indexScan.getRowType().getFieldCount(); i++) {
            RelColumnOrigin columnOrigin = mq.getColumnOrigin(indexScan, i);
            if (columnOrigin == null) {
                return null;
            }
            switchCols.add(columnOrigin.getColumnName());
        }

        int mainTableFieldCount = mainTableView.getRowType().getFieldCount();
        int indexScanFieldCount = indexScan.getRowType().getFieldCount();

        RelNode root = mainTableView.getPushedRelNode();
        RexBuilder rexBuilder = root.getCluster().getRexBuilder();
        if (root instanceof LogicalProject &&
            ((Project) root).getInput() instanceof LogicalFilter &&
            ((Filter) ((Project) root).getInput()).getInput() instanceof LogicalTableScan) {

            LogicalProject project = (LogicalProject) root;
            LogicalFilter filter = (LogicalFilter) ((LogicalProject) root).getInput();

            Pair<LogicalView, Map<String, Integer>> pair =
                addColumnsToProjectFilterScan(mainTableView, rexBuilder, project, filter, switchCols);
            if (pair == null) {
                return null;
            }
            Map<String, Integer> shiftMap = pair.getValue();
            LogicalView newMainTableView = pair.getKey();

            BKAJoin newJoin = BKAJoin.create(join.getTraitSet(), join.getLeft(), newMainTableView, join.getCondition(),
                join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(), ImmutableList.of(),
                join.getHints());
            newJoin.setHasSwitched(true);
            newMainTableView.setLookupInfo(newJoin);

            List<String> newProjectNames = new ArrayList<>();
            List<RexNode> newProjects = new ArrayList<>();

            for (String col : indexScan.getRowType().getFieldNames()) {
                Integer index = shiftMap.get(col);
                if (index == null) {
                    return null;
                }
                newProjects.add(rexBuilder.makeInputRef(newJoin, index + indexScanFieldCount));
                newProjectNames.add(col);
            }
            for (int i = 0; i < mainTableFieldCount; i++) {
                newProjects.add(rexBuilder.makeInputRef(newJoin, i + indexScanFieldCount));
                newProjectNames.add(newMainTableView.getRowType().getFieldNames().get(i));
            }
            RelDataType rowType = RexUtil.createStructType(
                newJoin.getCluster().getTypeFactory(), newProjects, newProjectNames, SqlValidatorUtil.F_SUGGESTER);
            RelTraitSet traitSet = newJoin.getTraitSet().simplify().replace(DrdsConvention.INSTANCE);
            return new PhysicalProject(newJoin.getCluster(), traitSet, newJoin, newProjects, rowType);
        }

        return null;
    }

    public static boolean checkIndexScan(LogicalIndexScan indexScan) {
        String mainTableName = indexScan.getMainTableName();

        if (StringUtils.isEmpty(mainTableName)) {
            return false;
        }
        if (indexScan.getTableNames().size() != 1) {
            return false;
        }
        RelNode root = indexScan.getPushedRelNode();
        if (root instanceof LogicalProject &&
            ((LogicalProject) root).getInput() instanceof LogicalFilter &&
            ((LogicalFilter) ((LogicalProject) root).getInput()).getInput() instanceof LogicalTableScan) {
            return true;
        }
        if (root instanceof LogicalFilter &&
            ((LogicalFilter) root).getInput() instanceof LogicalTableScan) {
            return true;
        }
        return false;
    }

    /**
     * Add columns to Project-Filter-Scan structure
     *
     * @param originView the original LogicalView
     * @param rexBuilder Rex expression builder
     * @param project logical projection node
     * @param filter logical filter node
     * @param colNames set of column names to be added
     * @return Pair containing new LogicalView and column mapping, null if failed
     */
    public static Pair<LogicalView, Map<String, Integer>> addColumnsToProjectFilterScan(LogicalView originView,
                                                                                        RexBuilder rexBuilder,
                                                                                        LogicalProject project,
                                                                                        LogicalFilter filter,
                                                                                        Set<String> colNames) {
        // Input parameter validation
        if (originView == null || rexBuilder == null || project == null || filter == null || colNames == null
            || colNames.isEmpty()) {
            return null;
        }

        RelNode filterInput = filter.getInput();
        if (!(filterInput instanceof LogicalTableScan)) {
            return null;
        }
        LogicalTableScan scan = (LogicalTableScan) filterInput;

        // Initialize new projection list and field name list
        List<RexNode> newProjects = new ArrayList<>(project.getProjects());
        List<String> newFieldNames = new ArrayList<>(project.getRowType().getFieldNames());

        // Use Map to improve lookup efficiency, avoiding repeated indexOf calls
        Map<String, Integer> existingFieldIndexMap = Maps.newHashMap();
        for (int i = 0; i < newFieldNames.size(); i++) {
            existingFieldIndexMap.put(newFieldNames.get(i), i);
        }

        // Get filter field name list for subsequent RexInputRef creation
        List<String> filterFieldNames = filter.getRowType().getFieldNames();
        Map<String, Integer> filterFieldIndexMap = Maps.newHashMap();
        for (int i = 0; i < filterFieldNames.size(); i++) {
            filterFieldIndexMap.put(filterFieldNames.get(i), i);
        }

        Map<String, Integer> columnShiftMap = Maps.newHashMap();

        for (String originalColName : colNames) {
            String finalColName = originalColName;
            Integer existingIndex = existingFieldIndexMap.get(originalColName);

            if (existingIndex != null) {
                // Column already exists in projection
                RexNode existingProjection = project.getProjects().get(existingIndex);
                if (existingProjection instanceof RexInputRef) {
                    // If it's a simple input reference, use existing index directly
                    columnShiftMap.put(originalColName, existingIndex);
                    continue;
                } else {
                    // If it's a complex expression, generate new unique column name
                    finalColName = generateUniqueColumnName(originalColName, existingFieldIndexMap.keySet());
                }
            }

            // Check if the column exists in the filter
            Integer filterColumnIndex = filterFieldIndexMap.get(originalColName);
            if (filterColumnIndex == null) {
                // Skip processing if column doesn't exist in filter
                continue;
            }

            // Add new projection column
            int newProjectIndex = newProjects.size();
            columnShiftMap.put(originalColName, newProjectIndex);
            newFieldNames.add(finalColName);
            newProjects.add(rexBuilder.makeInputRef(filter, filterColumnIndex));
            existingFieldIndexMap.put(finalColName, newProjectIndex);
        }

        // Create new logical projection and logical view
        LogicalProject newProject = LogicalProject.create(filter, newProjects, newFieldNames);
        LogicalView newLogicalView = LogicalView.create(scan, scan.getTable());
        newLogicalView.push(filter);
        newLogicalView.push(newProject);
        newLogicalView.setIsMGetEnabled(true);
        newLogicalView.setLockMode(originView.getLockMode());
        newLogicalView.setInToUnionAll(originView.isInToUnionAll());

        newLogicalView = newLogicalView.copy(newLogicalView.getTraitSet().simplify().replace(DrdsConvention.INSTANCE));

        return Pair.of(newLogicalView, columnShiftMap);
    }

    /**
     * Generate unique column name to avoid conflicts with existing column names
     *
     * @param baseName base column name
     * @param existingNames set of existing column names
     * @return unique column name
     */
    public static String generateUniqueColumnName(String baseName, Set<String> existingNames) {
        if (!existingNames.contains(baseName)) {
            return baseName;
        }

        int suffix = 0;
        String candidateName;
        do {
            candidateName = baseName + suffix;
            suffix++;
        } while (existingNames.contains(candidateName));

        return candidateName;
    }
}