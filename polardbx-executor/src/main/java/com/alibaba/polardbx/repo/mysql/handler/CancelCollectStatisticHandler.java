package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.CancelCollectStatisticSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCancelCollectStatistic;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author pangzhaoxing
 */
public class CancelCollectStatisticHandler extends HandlerCommon {

    public CancelCollectStatisticHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlCancelCollectStatistic sqlCancelCollectStatistic =
            (SqlCancelCollectStatistic) ((LogicalDal) logicalPlan).getNativeSqlNode();
        if (sqlCancelCollectStatistic.getConnectionIds() == null) {
            return new AffectRowCursor(0);
        }
        List<Long> connectionIds = sqlCancelCollectStatistic.getConnectionIds()
            .stream()
            .map(item -> item.longValue(true))
            .collect(Collectors.toList());
        String connIdsStr = CancelCollectStatisticSyncAction.buildConnIdListStr(connectionIds);

        List<List<Map<String, Object>>> res = SyncManagerHelper.syncIgnoreExceptions(
            new CancelCollectStatisticSyncAction(connIdsStr), SyncScope.ALL);
        int affectRow = 0;
        for (List<Map<String, Object>> list : res) {
            for (Map<String, Object> map : list) {
                affectRow += Long.parseLong(map.get("AFFECT_ROW").toString());
            }
        }
        return new AffectRowCursor(affectRow);
    }
}
