package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.gms.node.Node;
import com.google.common.collect.Multimap;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class HybridNodeSelector implements NodeSelector {

    private final ColumnarNodeSelector columnarNodeSelector;

    private final SimpleNodeSelector simpleNodeSelector;

    public HybridNodeSelector(SimpleNodeSelector simpleNodeSelector, ColumnarNodeSelector columnarNodeSelector) {
        this.simpleNodeSelector = simpleNodeSelector;
        this.columnarNodeSelector = columnarNodeSelector;
    }

    @Override
    public Node selectCurrentNode() {
        return simpleNodeSelector.selectCurrentNode();
    }

    @Override
    public List<Node> selectRandomNodes(int limit) {
        return simpleNodeSelector.selectRandomNodes(limit);
    }

    @Override
    public Multimap<Node, Split> computeAssignments(List<Split> splits, List<RemoteTask> existingTasks) {
        return simpleNodeSelector.computeAssignments(splits, existingTasks);
    }

    @Override
    public List<Node> getOrderedNode() {
        return simpleNodeSelector.getOrderedNode();
    }

    public ColumnarNodeSelector getColumnarNodeSelector() {
        return columnarNodeSelector;
    }
}
