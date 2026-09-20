package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

/**
 * Sync action to query all gray baseline plans from the current compute node.
 * This action retrieves gray plan information and returns it as a result cursor.
 *
 * @author jilong.ljl
 */
public class BaselineQueryAllGraySyncAction implements IGmsSyncAction {

    private static final String TABLE_NAME = "gray_baselines";
    private static final String COLUMN_COMPUTE_NODE = "COMPUTE_NODE";
    private static final String COLUMN_BASELINES = "BASELINES";

    public BaselineQueryAllGraySyncAction() {
    }

    /**
     * Executes the sync action to query all gray baseline plans.
     *
     * @return ResultCursor containing compute node ID and gray plan JSON data
     */
    @Override
    public ResultCursor sync() {
        // Get gray plan information from PlanManager
        String grayPlanJson = PlanManager.getInstance().getGrayPlan();

        // Create result cursor with schema
        ArrayResultCursor resultCursor = createResultCursor();

        // Add row with instance ID and gray plan data
        resultCursor.addRow(new Object[] {TddlNode.getHost() + ":" + TddlNode.getPort(), grayPlanJson});

        return resultCursor;
    }

    /**
     * Creates and initializes the result cursor with appropriate schema.
     *
     * @return Initialized ArrayResultCursor
     */
    private ArrayResultCursor createResultCursor() {
        ArrayResultCursor cursor = new ArrayResultCursor(TABLE_NAME);
        cursor.addColumn(COLUMN_COMPUTE_NODE, DataTypes.StringType);
        cursor.addColumn(COLUMN_BASELINES, DataTypes.StringType);
        cursor.initMeta();
        return cursor;
    }
}

