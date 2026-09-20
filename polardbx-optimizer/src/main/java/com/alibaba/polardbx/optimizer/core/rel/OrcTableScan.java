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

import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.config.meta.TableScanIOEstimator;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.config.meta.CostModelWeight.TUPLE_HEADER_SIZE;

/**
 * @author chenzilin
 */
public class OrcTableScan extends TableScan {

    private ImmutableList<Integer> outProjects;

    private ImmutableList<RexNode> filters;

    private ImmutableList<Integer> inProjects;

    private ImmutableList<String> inProjectNames;

    private RelDataType inputProjectRowType;

    private RelNode nodeForMetaQuery;

    OSSTableScan.OSSIndexContext indexContext;

    OrcTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                 RelOptTable table, List<Integer> outProjects, List<RexNode> filters,
                 List<Integer> inProjects, SqlNodeList hints, SqlNode indexNode, RexNode flashback,
                 SqlOperator flashbackOperator, SqlNode partitions) {
        super(cluster, traitSet, table, hints, indexNode, flashback, flashbackOperator, partitions);
        this.outProjects = ImmutableList.copyOf(outProjects);
        this.filters = ImmutableList.copyOf(filters);
        this.inProjects = ImmutableList.copyOf(inProjects);
        this.indexContext = null;
    }

    public static OrcTableScan create(RelOptCluster cluster, RelOptTable table,
                                      SqlNodeList hints, SqlNode indexNode, RexNode flashback,
                                      SqlOperator flashbackOperator,
                                      SqlNode partitions) {
        return new OrcTableScan(cluster, cluster.traitSetOf(Convention.NONE), table,
            ImmutableIntList.identity(table.getRowType().getFieldCount()),
            ImmutableList.of(), ImmutableIntList.identity(table.getRowType().getFieldCount()),
            hints, indexNode, flashback, flashbackOperator, partitions);
    }

    public OrcTableScan copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OrcTableScan(getCluster(), traitSet, table, outProjects, filters, inProjects,
            hints, indexNode, flashback, flashbackOperator, partitions);
    }

    public void push(RelNode relNode) {
        if (relNode instanceof LogicalFilter) {
            pushFilter((LogicalFilter) relNode);
        } else if (relNode instanceof LogicalProject) {
            pushProject((LogicalProject) relNode);
        }
        this.nodeForMetaQuery = null;
    }

    /**
     * Pushes a LogicalFilter down into this OrcTableScan by merging its filter conditions
     * with the existing filters, adjusting column references as needed.
     * <p>
     * This method combines the existing filters with new filter conditions from the
     * LogicalFilter node, ensuring that column indices are properly mapped between
     * the different projection layers.
     *
     * @param filter The LogicalFilter containing additional filter conditions to be pushed down
     */
    public void pushFilter(LogicalFilter filter) {
        // Create a mapping from current output project positions to original input column indices
        Map<Integer, Integer> shiftMap = Maps.newHashMap();
        for (int i = 0; i < outProjects.size(); i++) {
            shiftMap.put(i, outProjects.get(i));
        }

        // Build a new filter list by combining existing filters with the new filters
        // The new filters are adjusted using the shiftMap to align column references
        ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        builder.addAll(filters)                          // Add existing filters
            .addAll(RexUtil.shift(filter.getChildExps(), shiftMap)); // Add adjusted new filters
        this.filters = builder.build();                  // Update the filters list
    }

    /**
     * Push down a Project operation into this OrcTableScan, optimizing column references
     * and adjusting filters accordingly.
     * <p>
     * This method reorganizes the scan's column projections by:
     * 1. Identifying the original column references needed by the project operation
     * 2. Collecting all columns required by existing filters
     * 3. Creating a new minimal input projection that includes all necessary columns
     * 4. Adjusting filter expressions to match the new column layout
     * 5. Updating output projections to reflect the new column positions
     *
     * @param project The LogicalProject operation to push down containing desired output expressions
     */
    public void pushProject(Project project) {
        // Extract the original column indices referenced by the project expressions
        List<Integer> originCol = Lists.newArrayList();
        for (int ref : getColumnRefProjects(project.getProjects())) {
            // Map the projected column reference to the actual input column index
            originCol.add(inProjects.get(outProjects.get(ref)));
        }

        // Collect all column references that need to be preserved
        // Start with columns needed by the project operation
        Set<Integer> refs = Sets.newTreeSet(originCol);
        // Add columns referenced by existing filters to ensure they're available
        for (RexInputRef ref : RexUtil.findAllIndex(filters)) {
            refs.add(inProjects.get(ref.getIndex()));
        }

        // Create new input projects list from the collected column references (automatically sorted)
        List<Integer> newInProjects = new ArrayList<>(refs);

        // Build mapping from original column indices to their new positions in newInProjects
        Map<Integer, Integer> colMapAfterIn = Maps.newHashMap();
        for (int i = 0; i < newInProjects.size(); i++) {
            colMapAfterIn.put(newInProjects.get(i), i);
        }

        // Create shift mapping to adjust filter expressions to new column positions
        Map<Integer, Integer> shiftMap = Maps.newHashMap();
        for (int i = 0; i < inProjects.size(); i++) {
            // Map old position to new position, or -1 if column is no longer needed
            shiftMap.put(i, colMapAfterIn.getOrDefault(inProjects.get(i), -1));
        }
        // Adjust filter expressions according to the new column mapping
        List<RexNode> newFilters = RexUtil.shift(filters, shiftMap);

        // Build new output projects list based on the new column positions
        List<Integer> newOutProjects = Lists.newArrayList();
        for (int ref : originCol) {
            // Map each originally referenced column to its new position
            newOutProjects.add(colMapAfterIn.get(ref));
        }

        // Update the scan's projection and filter information with the new mappings
        this.outProjects = ImmutableList.copyOf(newOutProjects);
        this.filters = ImmutableList.copyOf(newFilters);
        this.inProjects = ImmutableList.copyOf(newInProjects);
    }

    private List<Integer> getColumnRefProjects(List<RexNode> projects) {
        return projects.stream().map(expr -> ((RexInputRef) expr).getIndex()).collect(Collectors.toList());
    }

    public void setIndexAble(OSSTableScan.OSSIndexContext indexContext) {
        this.indexContext = indexContext;
    }

    @Override
    public RelDataType deriveRowType() {
        return getNodeForMetaQuery().getRowType();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("filters", filters, !filters.isEmpty())
            .itemIf("projects", inProjects, !inProjects.isEmpty());
    }

    /**
     * get the number of group to be tested
     *
     * @param estimateRowCount row count to be read
     * @param totalRowCount total row count of all groups
     * @param size size of each group
     * @return number of groups have to be tested further
     */
    public long getGroupNumber(double estimateRowCount, double totalRowCount, int size) {
        int totalPage = (int) Math.ceil(totalRowCount / size);
        // the groups have to read
        int remainPage = totalPage;
        if (indexContext != null) {
            if (indexContext.isPrimary()) {
                remainPage = (int) Math.ceil(totalPage * (estimateRowCount / totalRowCount));
            } else {
                /**
                 * an estimation of the page has to be read
                 * it is assumed that the data has no skew and each row is queried in equal probability
                 * the formula is pageNum*(1-(1-row/totalRow)^pageSize, which is implemented by Fast Exponentiation
                 */
                double base = 1 - Math.min(1D, estimateRowCount / totalRowCount);
                double pow = 1D;
                int n = size;
                while (n > 0) {
                    if (n % 2 == 1) {
                        pow *= base;
                    }
                    base *= base;
                    n >>= 1;
                }
                remainPage = (int) Math.min(totalPage, Math.ceil(totalPage * (1 - pow)));
            }
        }
        return remainPage;
    }

    public static long getGroupNumberForRandomDistribution(double estimateRowCount, double totalRowCount, int size) {
        int totalPage = (int) Math.ceil(totalRowCount / size);
        // the groups have to read
        int remainPage = totalPage;
        /**
         * an estimation of the page has to be read
         * it is assumed that the data has no skew and each row is queried in equal probability
         * the formula is pageNum*(1-(1-row/totalRow)^pageSize, which is implemented by Fast Exponentiation
         */
        double base = 1 - Math.min(1D, estimateRowCount / totalRowCount);
        double pow = 1D;
        int n = size;
        while (n > 0) {
            if (n % 2 == 1) {
                pow *= base;
            }
            base *= base;
            n >>= 1;
        }
        remainPage = (int) Math.min(totalPage, Math.ceil(totalPage * (1 - pow)));
        return remainPage;
    }

    public RelOptCost getShardedReadCost(RelOptPlanner planner, double estimateRowCount, double totalRowCount) {
        long remainPage = getGroupNumber(estimateRowCount, totalRowCount, (int) CostModelWeight.OSS_PAGE_SIZE);

        remainPage = Math.max(1, remainPage);
        double rowCount = remainPage * CostModelWeight.OSS_PAGE_SIZE;
        double io = Math.ceil(TUPLE_HEADER_SIZE + TableScanIOEstimator.estimateRowSize(getInputProjectRowType())
            * remainPage * CostModelWeight.OSS_PAGE_SIZE);
        return planner.getCostFactory().makeCost(rowCount, 0, 0, io, rowCount);
    }

    public ImmutableList<Integer> getOutProjects() {
        return outProjects;
    }

    public ImmutableList<RexNode> getFilters() {
        return filters;
    }

    public List<RexNode> getOriFilters() {
        Map<Integer, Integer> shiftMap = Maps.newHashMap();
        for (int i = 0; i < inProjects.size(); i++) {
            shiftMap.put(i, inProjects.get(i));
        }
        return RexUtil.shift(filters, shiftMap);
    }

    public ImmutableList<Integer> getInProjects() {
        return inProjects;
    }

    public ImmutableList<String> getInputProjectName() {
        if (inProjectNames != null) {
            return inProjectNames;
        }
        inProjectNames =
            ImmutableList.copyOf(
                getInProjects().stream().map(idx -> table.getRowType().getFieldList().get(idx).getName())
                    .collect(Collectors.toList()));
        return inProjectNames;
    }

    public RelDataType getInputProjectRowType() {
        if (inputProjectRowType != null) {
            return inputProjectRowType;
        }
        final RelOptCluster cluster = getCluster();
        inputProjectRowType =
            RexUtil.createStructType(cluster.getTypeFactory(),
                getInProjects().stream()
                    .map(idx -> new RexInputRef(idx, table.getRowType().getFieldList().get(idx).getType()))
                    .collect(Collectors.toList()),
                getInProjects().stream().map(idx -> table.getRowType().getFieldList().get(idx).getName())
                    .collect(Collectors.toList()),
                SqlValidatorUtil.F_SUGGESTER);
        return inputProjectRowType;
    }

    public List<DataType<?>> getInProjectsDataType() {
        List<DataType<?>> inputTypes = new ArrayList<>();
        ImmutableList<Integer> inProjects = getInProjects();
        for (int i = 0; i < inProjects.size(); i++) {
            List<RelDataTypeField> relDataTypeList = getTable().getRowType().getFieldList();
            RelDataType relDataType = relDataTypeList.get(inProjects.get(i)).getType();
            DataType dt = DataTypeUtil.calciteToDrdsType(relDataType);
            inputTypes.add(dt);
        }
        return inputTypes;
    }

    public RelNode getNodeForMetaQuery() {
        if (nodeForMetaQuery == null) {
            synchronized (this) {
                if (filters.isEmpty() && inProjects.isEmpty() && outProjects.isEmpty()) {
                    nodeForMetaQuery =
                        LogicalTableScan.create(getCluster(), table, hints, indexNode, flashback, flashbackOperator,
                            partitions);
                } else if (!filters.isEmpty() && inProjects.isEmpty() && outProjects.isEmpty()) {
                    LogicalTableScan logicalTableScan =
                        LogicalTableScan.create(getCluster(), table, hints, indexNode, flashback, flashbackOperator,
                            partitions);
                    LogicalFilter logicalFilter = LogicalFilter.create(logicalTableScan, filters.get(0));
                    nodeForMetaQuery = logicalFilter;
                } else if (filters.isEmpty() && !inProjects.isEmpty() && outProjects.isEmpty()) {
                    LogicalTableScan logicalTableScan =
                        LogicalTableScan.create(getCluster(), table, hints, indexNode, flashback, flashbackOperator,
                            partitions);
                    LogicalProject inLogicalProject = LogicalProject.create(logicalTableScan,
                        inProjects.stream()
                            .map(idx -> new RexInputRef(idx, table.getRowType().getFieldList().get(idx).getType()))
                            .collect(Collectors.toList()),
                        inProjects.stream().map(idx -> table.getRowType().getFieldList().get(idx).getName())
                            .collect(Collectors.toList()));
                    nodeForMetaQuery = inLogicalProject;
                } else if (!filters.isEmpty() && !inProjects.isEmpty() && outProjects.isEmpty()) {
                    LogicalTableScan logicalTableScan =
                        LogicalTableScan.create(getCluster(), table, hints, indexNode, flashback, flashbackOperator,
                            partitions);
                    LogicalProject inLogicalProject = LogicalProject.create(logicalTableScan,
                        inProjects.stream()
                            .map(idx -> new RexInputRef(idx, table.getRowType().getFieldList().get(idx).getType()))
                            .collect(Collectors.toList()),
                        inProjects.stream().map(idx -> table.getRowType().getFieldList().get(idx).getName())
                            .collect(Collectors.toList()));
                    LogicalFilter logicalFilter = LogicalFilter.create(inLogicalProject, filters.get(0));
                    nodeForMetaQuery = logicalFilter;
                } else if (filters.isEmpty() && inProjects.isEmpty() && !outProjects.isEmpty()) {
                    LogicalTableScan logicalTableScan =
                        LogicalTableScan.create(getCluster(), table, hints, indexNode, flashback, flashbackOperator,
                            partitions);
                    LogicalProject inLogicalProject = LogicalProject.create(logicalTableScan,
                        outProjects.stream()
                            .map(idx -> new RexInputRef(idx, table.getRowType().getFieldList().get(idx).getType()))
                            .collect(Collectors.toList()),
                        outProjects.stream().map(idx -> table.getRowType().getFieldList().get(idx).getName())
                            .collect(Collectors.toList()));
                    nodeForMetaQuery = inLogicalProject;
                } else if (!filters.isEmpty() && inProjects.isEmpty() && !outProjects.isEmpty()) {
                    LogicalTableScan logicalTableScan =
                        LogicalTableScan.create(getCluster(), table, hints, indexNode, flashback, flashbackOperator,
                            partitions);
                    LogicalFilter logicalFilter = LogicalFilter.create(logicalTableScan, filters.get(0));
                    LogicalProject outLogicalProject = LogicalProject.create(logicalFilter,
                        outProjects.stream()
                            .map(idx -> new RexInputRef(idx, table.getRowType().getFieldList().get(idx).getType()))
                            .collect(Collectors.toList()),
                        outProjects.stream().map(idx -> table.getRowType().getFieldList().get(idx).getName())
                            .collect(Collectors.toList()));
                    nodeForMetaQuery = outLogicalProject;
                } else if (filters.isEmpty() && !inProjects.isEmpty() && !outProjects.isEmpty()) {
                    LogicalTableScan logicalTableScan =
                        LogicalTableScan.create(getCluster(), table, hints, indexNode, flashback, flashbackOperator,
                            partitions);
                    LogicalProject inLogicalProject = LogicalProject.create(logicalTableScan,
                        inProjects.stream()
                            .map(idx -> new RexInputRef(idx, table.getRowType().getFieldList().get(idx).getType()))
                            .collect(Collectors.toList()),
                        inProjects.stream().map(idx -> table.getRowType().getFieldList().get(idx).getName())
                            .collect(Collectors.toList()));
                    LogicalProject outLogicalProject = LogicalProject.create(inLogicalProject,
                        outProjects.stream().map(idx -> new RexInputRef(idx,
                                inLogicalProject.getRowType().getFieldList().get(idx).getType()))
                            .collect(Collectors.toList()),
                        outProjects.stream().map(idx -> inLogicalProject.getRowType().getFieldList().get(idx).getName())
                            .collect(Collectors.toList()));
                    nodeForMetaQuery = outLogicalProject;
                } else if (!filters.isEmpty() && !inProjects.isEmpty() && !outProjects.isEmpty()) {
                    LogicalTableScan logicalTableScan =
                        LogicalTableScan.create(getCluster(), table, hints, indexNode, flashback, flashbackOperator,
                            partitions);
                    LogicalProject inLogicalProject = LogicalProject.create(logicalTableScan,
                        inProjects.stream()
                            .map(idx -> new RexInputRef(idx, table.getRowType().getFieldList().get(idx).getType()))
                            .collect(Collectors.toList()),
                        inProjects.stream().map(idx -> table.getRowType().getFieldList().get(idx).getName())
                            .collect(Collectors.toList()));
                    LogicalFilter logicalFilter = LogicalFilter.create(inLogicalProject, filters.get(0));
                    LogicalProject outLogicalProject = LogicalProject.create(logicalFilter,
                        outProjects.stream().map(idx -> new RexInputRef(idx,
                                inLogicalProject.getRowType().getFieldList().get(idx).getType()))
                            .collect(Collectors.toList()),
                        outProjects.stream().map(idx -> inLogicalProject.getRowType().getFieldList().get(idx).getName())
                            .collect(Collectors.toList()));
                    nodeForMetaQuery = outLogicalProject;
                }
            }
        }
        return nodeForMetaQuery;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "OrcTableScan");
        pw.item("name", getTable().getQualifiedName());
        if (!filters.isEmpty()) {
            RexExplainVisitor visitor = new RexExplainVisitor(
                outProjects.isEmpty() ? getNodeForMetaQuery() : getNodeForMetaQuery().getInput(0));
            filters.get(0).accept(visitor);
            pw.item("filter", visitor.toSqlString());
        }
        pw.item("inProject", getInProjects().stream().map(idx -> table.getRowType().getFieldList().get(idx).getName())
            .collect(Collectors.toList()));
        pw.item("outProject",
            getOutProjects().stream().map(idx -> getInputProjectRowType().getFieldList().get(idx).getName())
                .collect(Collectors.toList()));
        return pw;
    }
}

