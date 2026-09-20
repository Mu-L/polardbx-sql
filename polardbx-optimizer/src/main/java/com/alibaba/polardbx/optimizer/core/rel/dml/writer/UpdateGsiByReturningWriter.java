package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableModifyBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * UPDATE on GSI in relocate returning execute
 * <p>
 * Presupposed input row constitution:
 * <pre>
 *  UPDATE t SET c1 = u1, ..., cm = um
 *
 *  Input row looks like below
 *
 *           after values from update returning
 *                      |
 *    [c1, ..., cn][u1, ..., un]
 *          |
 *  before values from update returning
 * </pre>
 */
public class UpdateGsiByReturningWriter extends AbstractSingleWriter implements DistinctWriter {

    /**
     * Operator , should only be UPDATE
     */
    protected final LogicalModify modify;
    /**
     * Column meta for sharding, identical to column metas in TableMeta
     */
    protected final List<ColumnMeta> skMetas;
    /**
     * Mapping for primary key in input row
     */
    protected final Mapping pkMapping;
    /**
     * Mapping for sharding key in input row
     */
    protected final Mapping skMapping;
    /**
     * Mapping for source value of SET in input row
     */
    protected Mapping updateSetMapping;
    /**
     * Mapping for columns used to group input rows, normally identical to pkMapping
     * if pkMapping and skMapping are identical, then groupingMapping is also identical to them
     * otherwise groupingMapping is the combination of pkMapping and skMapping
     */
    protected final Mapping groupingMapping;

    protected final boolean withoutPk;

    protected final TableMeta gsiMeta;

    public UpdateGsiByReturningWriter(RelOptTable targetTable, LogicalModify modify, List<ColumnMeta> skMetas,
                                      Mapping pkMapping, Mapping skMapping, Mapping updateSetMapping,
                                      Mapping groupingMapping, boolean withoutPk, TableMeta gsiMeta) {
        super(targetTable, modify.getOperation());
        this.modify = modify;
        this.skMetas = skMetas;
        this.pkMapping = pkMapping;
        this.skMapping = skMapping;
        this.updateSetMapping = updateSetMapping;
        this.groupingMapping = groupingMapping;
        this.withoutPk = withoutPk;
        this.gsiMeta = gsiMeta;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {
        if (Objects.requireNonNull(getOperation()) == TableModify.Operation.UPDATE && (
            GlobalIndexMeta.canWrite(ec, gsiMeta) || GlobalIndexMeta.canDelete(ec, gsiMeta))) {
            // DELETE_ONLY or WRITE_ONLY or PUBLIC ，according to ShardingModifyGsiWriter
            final RelOptTable targetTable = getTargetTable();
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);

            // Deduplicate
            final List<List<Object>> distinctRows = rowGenerator.apply(this);

            if (distinctRows.isEmpty()) {
                return new ArrayList<>();
            }

            final List<List<Object>> beforeValueRows = new ArrayList<>(Collections.emptyList());
            distinctRows.forEach(row -> {
                beforeValueRows.add(row.subList(0, row.size() / 2));
            });

            // targetDb: { targetTb: [{ rowIndex, [pk1, pk2] }] }
            final Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResult = BuildPlanUtils
                .buildResultForShardingTable(qn.left, qn.right, beforeValueRows, skMetas, skMapping, pkMapping, ec,
                    false);

            final PhyTableModifyBuilder builder = new PhyTableModifyBuilder();
            return builder.buildUpdateWithPk(modify, distinctRows, updateSetMapping, qn, shardResult, ec);
        }
        throw new AssertionError("Cannot handle operation " + getOperation().name());
    }

    public LogicalModify getModify() {
        return modify;
    }

    public List<ColumnMeta> getSkMetas() {
        return skMetas;
    }

    public Mapping getPkMapping() {
        return pkMapping;
    }

    public Mapping getSkMapping() {
        return skMapping;
    }

    public Mapping getUpdateSetMapping() {
        return updateSetMapping;
    }

    public void setUpdateSetMapping(Mapping updateSetMapping) {
        this.updateSetMapping = updateSetMapping;
    }

    @Override
    public Mapping getGroupingMapping() {
        return groupingMapping;
    }

    @Override
    public List<Writer> getInputs() {
        return Collections.emptyList();
    }
}
