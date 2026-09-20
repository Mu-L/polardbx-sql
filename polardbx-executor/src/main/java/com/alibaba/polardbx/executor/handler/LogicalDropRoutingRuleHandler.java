package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleAccessor;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalRoutingRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDropRoutingRule;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class LogicalDropRoutingRuleHandler extends HandlerCommon {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalDropRoutingRuleHandler.class);

    public LogicalDropRoutingRuleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalRoutingRule plan = (LogicalRoutingRule) logicalPlan;
        SqlDropRoutingRule sqlNode = (SqlDropRoutingRule) plan.getSqlDal();
        List<String> ruleNames =
            sqlNode.getRuleNames().stream().map((e) -> e.getSimple()).filter(x -> !x.isEmpty())
                .collect(Collectors.toList());
        if (ruleNames.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE,
                "Failed to DROP EMPTY ROUTING_RULE!");
        }
        validateUser(ruleNames, executionContext);
        return commitDrop(ruleNames, sqlNode.isIfExists());
    }

    protected void validateUser(List<String> ruleNames, ExecutionContext ec) {
        if (ec.isSuperUser()) {
            return;
        }
        String userName = ec.getPrivilegeContext().getUser();
        Set<String> ruleNameSet = new TreeSet<>(String::compareToIgnoreCase);
        ruleNameSet.addAll(ruleNames);
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            RoutingRuleAccessor routingRuleAccessor = RoutingRuleAccessor.create(metaDbConn);
            List<RoutingRuleRecord> records = routingRuleAccessor.query(InstIdUtil.getInstId());
            for (RoutingRuleRecord record : records) {
                if (ruleNameSet.contains(record.ruleName)) {
                    if (!record.userName.equalsIgnoreCase(userName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE,
                            "Failed to DROP ROUTING_RULE " + record.ruleName + " which does not belong to you");
                    }
                }
            }
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    protected Cursor commitDrop(List<String> ruleNames, boolean ifExists) {
        String dataId = MetaDbDataIdBuilder.getRoutingRuleDataId(InstIdUtil.getInstId());
        int affectRowCount;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            try {
                metaDbConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                RoutingRuleAccessor routingRuleAccessor = RoutingRuleAccessor.create(metaDbConn);
                MetaDbUtil.beginTransaction(metaDbConn);
                affectRowCount = routingRuleAccessor.delete(InstIdUtil.getInstId(), ruleNames);
                if (!ifExists) {
                    if (affectRowCount != ruleNames.size()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE,
                            "Failed to DROP ROUTING_RULE which does not exist");
                    }
                }
                MetaDbConfigManager.getInstance().notify(dataId, metaDbConn);
                MetaDbUtil.commit(metaDbConn);
            } catch (Throwable t) {
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(t), LOGGER, "delete routing rule failed");
                throw t;
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }
            MetaDbConfigManager.getInstance().sync(dataId);
            return new AffectRowCursor(affectRowCount);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }
}
