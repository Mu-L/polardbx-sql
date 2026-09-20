package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.google.common.collect.Sets;

import java.util.Set;

public enum RoutingType {
    FOLLOWER, STALE, ROW, COLUMNAR, HTAP;

    public static boolean containsColumnar(RoutingType routingType) {
        return routingType == COLUMNAR || routingType == HTAP;
    }

    public static boolean isFollower(RoutingType routingType) {
        return routingType == FOLLOWER || routingType == STALE;
    }

    public static boolean isRoutingType(String type) {
        for (RoutingType t : RoutingType.values()) {
            if (t.name().equalsIgnoreCase(type)) {
                return true;
            }
        }
        return false;
    }

    public static RoutingType getType(String type) {
        if (StringUtils.isEmpty(type)) {
            return null;
        }
        switch (type.toLowerCase()) {
        case "follower":
            return FOLLOWER;
        case "stale":
            return STALE;
        case "row":
            return ROW;
        case "columnar":
            return COLUMNAR;
        case "htap":
            return HTAP;
        default:
            return null;
        }
    }

    public static Set<OptimizerType> validOptimizerType(RoutingType routingType) {
        if (routingType == null) {
            return Sets.newHashSet(OptimizerType.SMP, OptimizerType.MPP, OptimizerType.COLUMNAR);
        }
        switch (routingType) {
        case STALE:
        case FOLLOWER:
        case ROW:
            return Sets.newHashSet(OptimizerType.SMP, OptimizerType.MPP);
        case COLUMNAR:
            return Sets.newHashSet(OptimizerType.COLUMNAR);
        case HTAP:
        default:
            return Sets.newHashSet(OptimizerType.SMP, OptimizerType.MPP, OptimizerType.COLUMNAR);
        }
    }

    public static RoutingType determineRoutingType(ExecutionContext ec, String templateId,
                                                   SqlParameterized sqlParameterized) {
        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_MANUAL_ROUTING)) {
            HtapTrace.addTrace(ec.getHtapTrace(), "ENABLE_MANUAL_ROUTING is disabled");
            return null;
        }
        // only routing for select statement
        if (!(sqlParameterized.getStmt() instanceof SQLSelectStatement)) {
            return null;
        }
        return RoutingRuleManager.getInstance().determineRoutingType(ec, templateId, sqlParameterized.getSql());
    }
}
