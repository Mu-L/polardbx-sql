package com.alibaba.polardbx.optimizer.core.planner.rule.holisticUnnest;

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;

import java.util.Map;

public class SubtreeChecker {
    private int timer;
    private final Map<Integer, Integer> inTime;
    private final Map<Integer, Integer> outTime;

    public SubtreeChecker() {
        this.timer = 0;
        this.inTime = Maps.newHashMap();
        this.outTime = Maps.newHashMap();
    }

    private void dfs(RelNode root) {
        inTime.put(root.getId(), timer++);
        for (RelNode child : root.getInputs()) {
            dfs(CBOUtil.stripHep(child));
        }
        if (root instanceof LogicalCTEConsumer) {
            dfs(CBOUtil.stripHep(((LogicalCTEConsumer) root).getInnerRel()));
        }
        outTime.put(root.getId(), timer++);
    }

    public boolean isInSubtree(RelNode target, RelNode root) {
        int targetId = CBOUtil.stripHep(target).getId();
        int rootId = CBOUtil.stripHep(root).getId();
        return inTime.get(rootId) <= inTime.get(targetId) && inTime.get(targetId) <= outTime.get(rootId);
    }

    public static SubtreeChecker build(RelNode root) {
        SubtreeChecker subtreeChecker = new SubtreeChecker();
        subtreeChecker.dfs(root);
        return subtreeChecker;
    }
}
