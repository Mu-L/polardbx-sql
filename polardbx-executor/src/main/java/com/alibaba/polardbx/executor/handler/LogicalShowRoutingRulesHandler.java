package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleAccessor;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class LogicalShowRoutingRulesHandler extends HandlerCommon {

    private static Class showRoutingRulesSyncActionClass;

    static {
        try {
            showRoutingRulesSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.ShowRoutingRulesSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public LogicalShowRoutingRulesHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        return handle(executionContext);
    }

    public Cursor handle(ExecutionContext executionContext) {
        ISyncAction showRoutingRulesAction;
        if (showRoutingRulesSyncActionClass == null) {
            throw new NotSupportException();
        }
        try {
            showRoutingRulesAction = (ISyncAction) showRoutingRulesSyncActionClass.getConstructor().newInstance();
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        List<List<Map<String, Object>>> routingRulesCount =
            syncRoutingRulesCount(showRoutingRulesAction, executionContext);
        Map<Long, Long> routingRulesCountMap = Maps.newHashMap();
        for (List<Map<String, Object>> oneSyncResult : routingRulesCount) {
            for (Map<String, Object> map : oneSyncResult) {
                routingRulesCountMap.merge(DataTypes.LongType.convertFrom(map.get("ID")),
                    DataTypes.LongType.convertFrom(map.get("COUNT")), Long::sum);
            }
        }

        ArrayResultCursor cursor = new ArrayResultCursor("SHOW_ROUTING_RULES");
        cursor.addColumn("ID", DataTypes.LongType);
        cursor.addColumn("RULE_NAME", DataTypes.StringType);
        cursor.addColumn("USER_NAME", DataTypes.StringType);
        cursor.addColumn("TEMPLATE_ID", DataTypes.StringType);
        cursor.addColumn("KEYWORDS", DataTypes.StringType);
        cursor.addColumn("ROUTING_TYPE", DataTypes.StringType);
        cursor.addColumn("CREATE_TIME", DataTypes.TimestampType);
        cursor.addColumn("HIT_COUNT", DataTypes.LongType);
        cursor.initMeta();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            RoutingRuleAccessor routingRuleAccessor = getRoutingAccessor();
            routingRuleAccessor.setConnection(metaDbConn);
            List<RoutingRuleRecord> allRecords = routingRuleAccessor.query(InstIdUtil.getInstId());
            for (RoutingRuleRecord record : allRecords) {
                long cnt = routingRulesCountMap.getOrDefault(record.id, 0L);
                if (cnt < 0) {
                    cnt = Long.MAX_VALUE;
                }
                cursor.addRow(new Object[] {
                    record.id, record.ruleName, record.userName, record.templateId,
                    record.keywords, record.routingType, record.createdTime, cnt});
            }
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }

        return cursor;
    }

    protected List<List<Map<String, Object>>> syncRoutingRulesCount(ISyncAction showRoutingRulesAction,
                                                                    ExecutionContext executionContext) {
        return SyncManagerHelper.syncIgnoreExceptions(showRoutingRulesAction, executionContext.getSchemaName(),
            SyncScope.CURRENT_ONLY);
    }

    protected RoutingRuleAccessor getRoutingAccessor() {
        return new RoutingRuleAccessor();
    }
}
