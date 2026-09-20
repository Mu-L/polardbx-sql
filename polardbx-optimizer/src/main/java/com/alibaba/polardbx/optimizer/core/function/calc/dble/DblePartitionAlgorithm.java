package com.alibaba.polardbx.optimizer.core.function.calc.dble;

import com.alibaba.polardbx.common.properties.DynamicConfig;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author chenghui.lch
 */
public abstract class DblePartitionAlgorithm extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    private Integer dataNodeCount;

    public DblePartitionAlgorithm() {
    }

    public Object compute(Object[] args) {
        Object val = args[0];
        if (val == null) {
            return calculateWithCheck(null);
        }
        if (val instanceof String) {
            return calculateWithCheck((String) val);
        } else if (val instanceof Long || val instanceof Integer) {
            String valStr = String.valueOf(val);
            return calculateWithCheck(valStr);
        } else if (val instanceof Date) {
            Date dateVal = (Date) val;
            String dateFormat = getDateFormat();
            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
            String formattedDateStr = sdf.format(dateVal);
            return calculateWithCheck(formattedDateStr);
        } else {
            String valStr = String.valueOf(val);
            return calculateWithCheck(valStr);
        }
    }

    protected Object calculateWithCheck(String columnValue) {
        Object nodeIndex = calculate(columnValue);
        if (DynamicConfig.getInstance().isEnableDbleRouteResultCheck()) {
            checkIfNodeIndexValid(nodeIndex, columnValue);
        }
        return nodeIndex;
    }

    protected void checkIfNodeIndexValid(Object nodeIdxRs, String columnValue) {
        if (nodeIdxRs == null) {
            String msg =
                String.format("can't find any valid data node: nodeIndex is null, columnValue is %s", columnValue);
            throw new IllegalArgumentException(msg);
        }
        boolean enableDataNodeIndexCheck = DynamicConfig.getInstance().isEnableDbleCheckDataNodeIndexRouting();
        if (enableDataNodeIndexCheck) {
            if (nodeIdxRs instanceof Integer) {
                Integer routedNodeId = (Integer) nodeIdxRs;
                if (dataNodeCount != null) {
                    if (routedNodeId < 0 || routedNodeId >= dataNodeCount) {
                        String msg = String.format("can't find any valid data node: nodeIndex is %s, columnValue is %s",
                            routedNodeId, columnValue);
                        throw new IllegalArgumentException(msg);
                    }
                }
            }
        }
    }

    protected String getDateFormat() {
        return null;
    }

    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        throw new UnsupportedOperationException();
    }

    public Integer getDataNodeCount() {
        return dataNodeCount;
    }

    public void setDataNodeCount(Integer dataNodeCount) {
        this.dataNodeCount = dataNodeCount;
    }
}
