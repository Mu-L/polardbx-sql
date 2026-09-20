package com.alibaba.polardbx.optimizer.core.planner.rule.holisticUnnest;

import com.alibaba.polardbx.common.utils.UnionFind;
import com.google.common.collect.Maps;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.Map;

public class Equality {
    private UnionFind<Integer> unionFind;
    private int counter;
    private Map<CorDef, Integer> corDefIdMap;
    private Map<Integer, Integer> colRefIdMap;

    public Equality() {
        this.unionFind = new UnionFind<>();
        this.counter = 0;
        this.corDefIdMap = Maps.newHashMap();
        this.colRefIdMap = Maps.newHashMap();
    }

    public void shiftColRef(Map<Integer, Integer> shift) {
        Map<Integer, Integer> newColRefIdMap = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> entry : colRefIdMap.entrySet()) {
            if (shift.containsKey(entry.getKey())) {
                newColRefIdMap.put(shift.get(entry.getKey()), entry.getValue());
            }
        }
        colRefIdMap = newColRefIdMap;
    }

    public Integer getId(Integer colRef) {
        Integer id = colRefIdMap.computeIfAbsent(colRef, k -> counter++);
        unionFind.add(id);
        return id;
    }

    public Integer getId(CorDef corDef) {
        Integer id = corDefIdMap.computeIfAbsent(corDef, k -> counter++);
        unionFind.add(id);
        return id;
    }

    private Integer getId(RexNode node) {
        if (node instanceof RexInputRef) {
            return getId(((RexInputRef) node).getIndex());
        } else if (node instanceof RexFieldAccess) {
            final RexNode ref = ((RexFieldAccess) node).getReferenceExpr();
            if (!(ref instanceof RexCorrelVariable)) {
                return null;
            }
            final RexCorrelVariable corVar = (RexCorrelVariable) ref;
            return getId(
                CorDef.create(corVar.getId(), ((RexFieldAccess) node).getField().getIndex()));
        }
        return null;
    }

    public void union(RexNode a, RexNode b) {
        Integer idA = getId(a);
        Integer idB = getId(b);
        if (idA == null || idB == null) {
            return;
        }
        unionFind.union(idA, idB);
    }

    public UnionFind<Integer> getUnionFind() {
        return unionFind;
    }

    public Equality copy() {
        Equality newEquality = new Equality();
        newEquality.unionFind = this.unionFind.copy();
        newEquality.counter = this.counter;
        newEquality.corDefIdMap = Maps.newHashMap(corDefIdMap);
        newEquality.colRefIdMap = Maps.newHashMap(colRefIdMap);
        return newEquality;
    }
}
