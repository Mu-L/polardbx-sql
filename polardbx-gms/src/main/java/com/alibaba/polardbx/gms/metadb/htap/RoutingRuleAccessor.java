package com.alibaba.polardbx.gms.metadb.htap;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public class RoutingRuleAccessor extends AbstractAccessor {
    private static final String ROUTING_RULE = wrap(GmsSystemTables.ROUTING_RULE);

    private static final String USER_DEFINE_COLUMNS =
        "`rule_name`, `user_name`, `template_id`, `keywords`, `routing_type`";

    private static final String FULL_COLUMNS =
        USER_DEFINE_COLUMNS + ", `gmt_created`";

    private static final String SELECT_BY_INST =
        "select `id`, " + FULL_COLUMNS + " from " + ROUTING_RULE
            + " where `inst_id` = ?";

    private static final String LOCK_BY_INST_AND_RULE =
        "select `id`, " + FULL_COLUMNS + " from " + ROUTING_RULE
            + " where `inst_id` = ? and `rule_name` = ? for update";

    private static final String DELETE_BY_INST_AND_RULE = "delete from " + ROUTING_RULE
        + " where `inst_id` = '%s' and `rule_name` in (%s)";

    private static final String DELETE_BY_USER = "delete from " + ROUTING_RULE
        + " where `user_name` in (%s)";

    private static final String INSERT_RULE = "insert into " + ROUTING_RULE + "(`inst_id`, " + USER_DEFINE_COLUMNS
        + ") values (?, ?, ?, ?, ?, ?)";

    public int insert(RoutingRuleRecord record) {
        return super.insert(INSERT_RULE, ROUTING_RULE, record.buildInsertParams());
    }

    public int delete(String instId, Collection<String> ruleNames) {
        try {
            return MetaDbUtil.delete(
                String.format(DELETE_BY_INST_AND_RULE, instId, concat(ruleNames)), connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete from",
                ROUTING_RULE,
                e.getMessage());
        }
    }

    public int deleteByUser(List<String> ruleNames) {
        try {
            return MetaDbUtil.delete(
                String.format(DELETE_BY_USER, concat(ruleNames)),
                connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete from",
                ROUTING_RULE,
                e.getMessage());
        }
    }

    public List<RoutingRuleRecord> query(String instId) {
        return super.query(SELECT_BY_INST, ROUTING_RULE, RoutingRuleRecord.class,
            MetaDbUtil.buildStringParameters(new String[] {instId}));
    }

    public List<RoutingRuleRecord> lockRule(String instId, String ruleName) {
        return super.query(LOCK_BY_INST_AND_RULE, ROUTING_RULE, RoutingRuleRecord.class,
            MetaDbUtil.buildStringParameters(new String[] {instId, ruleName}));
    }

    public static RoutingRuleAccessor create(Connection connection) {
        RoutingRuleAccessor accessor = new RoutingRuleAccessor();
        accessor.setConnection(connection);
        return accessor;
    }
}
