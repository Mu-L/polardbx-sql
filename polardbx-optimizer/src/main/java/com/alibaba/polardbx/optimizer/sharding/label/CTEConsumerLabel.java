package com.alibaba.polardbx.optimizer.sharding.label;

import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEConsumer;
import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CTEConsumer;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.mapping.Mapping;

import javax.annotation.Nonnull;
import java.util.List;

public class CTEConsumerLabel extends AbstractLabel {

    protected CTEConsumerLabel(@Nonnull CTEConsumer rel, List<Label> inputs) {
        super(LabelType.CTE_CONSUMER, rel, inputs);
    }

    public CTEConsumerLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType,
                            Mapping columnMapping, RelDataType currentBaseRowType, PredicateNode pullUp,
                            PredicateNode pushdown, PredicateNode[] columnConditionMap,
                            List<PredicateNode> predicates) {
        super(type,
            inputs,
            rel,
            fullRowType,
            columnMapping,
            currentBaseRowType,
            pullUp,
            pushdown,
            columnConditionMap,
            predicates);
    }

    public static CTEConsumerLabel create(@Nonnull CTEConsumer cteConsumer) {
        return new CTEConsumerLabel(cteConsumer, ImmutableList.of());
    }

    @Override
    public Label copy(List<Label> inputs) {
        return new CTEConsumerLabel(getType(),
            inputs,
            rel,
            fullRowType,
            columnMapping,
            currentBaseRowType,
            pullUp,
            pushdown,
            columnConditionMap,
            predicates);
    }

    @Override
    public Label accept(LabelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public String toString() {
        final PhysicalCTEConsumer cteConsumer = getRel();
        return super.toString() + ", cteId:" + cteConsumer.getCteId();
    }
}
