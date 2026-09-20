package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESSING;
import static com.alibaba.polardbx.gms.module.Module.SPM;

public class BaselineGraySyncAction implements ISyncAction {

    private String schemaName;

    private Integer baselineId;

    private Integer planInfoId;

    private Integer grayRatio;

    public BaselineGraySyncAction(String schemaName, Integer baselineId, int planInfoId, int grayRatio) {
        this.schemaName = schemaName;
        this.baselineId = baselineId;
        this.planInfoId = planInfoId;
        this.grayRatio = grayRatio;
    }

    @Override
    public ResultCursor sync() {
        ModuleLogInfo.getInstance().logRecord(SPM, PROCESSING, new String[]
                {"gray plan :", schemaName + "," + baselineId + "," + planInfoId + "," + grayRatio},
            NORMAL);
        int oldGrayRatio = PlanManager.getInstance().grayPlan(schemaName, baselineId, planInfoId, grayRatio);
        ArrayResultCursor result = new ArrayResultCursor("baseline");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("STATUS", DataTypes.StringType);
        result.addColumn("OLD_GRAY_RATIO", DataTypes.IntegerType);

        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            oldGrayRatio == -1 ? "target plan not found" : "success",
            oldGrayRatio
        });
        return result;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public Integer getBaselineId() {
        return baselineId;
    }

    public void setBaselineId(Integer baselineId) {
        this.baselineId = baselineId;
    }

    public Integer getPlanInfoId() {
        return planInfoId;
    }

    public void setPlanInfoId(Integer planInfoId) {
        this.planInfoId = planInfoId;
    }

    public Integer getGrayRatio() {
        return grayRatio;
    }

    public void setGrayRatio(Integer grayRatio) {
        this.grayRatio = grayRatio;
    }
}

