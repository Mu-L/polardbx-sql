package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.statistic.CollectStatisticProgress;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * @author pangzhaoxing
 */
public class CancelCollectStatisticSyncAction implements ISyncAction {

    String connIdsStr;

    private static final String SPLIT = ",";

    public CancelCollectStatisticSyncAction(String connIdsStr) {
        this.connIdsStr = connIdsStr;
    }

    @Override
    public ResultCursor sync() {
        int affectRow = 0;
        if (connIdsStr != null) {
            for (long connectionId : parseConnIds(connIdsStr)) {
                if (CollectStatisticProgress.cancelCollectStatistic(connectionId)) {
                    CollectStatisticProgress.removeCollectStatisticProgress(connectionId);
                    affectRow += 1;
                }
            }
        }
        ArrayResultCursor cursor = new ArrayResultCursor("CANCEL_COLLECT_STATISTIC");
        cursor.addColumn("AFFECT_ROW", DataTypes.LongType);
        cursor.addRow(new Object[] {affectRow});
        return cursor;
    }

    public String getConnIdsStr() {
        return connIdsStr;
    }

    public void setConnIdsStr(String connIdsStr) {
        this.connIdsStr = connIdsStr;
    }

    public static String buildConnIdListStr(List<Long> connectionIds) {
        StringJoiner sj = new StringJoiner(SPLIT);
        for (Long connectionId : connectionIds) {
            sj.add(String.valueOf(connectionId));
        }
        return sj.toString();
    }

    private static List<Long> parseConnIds(String connIdsStr) {
        return Arrays.stream(connIdsStr.split(SPLIT)).map(Long::parseLong).collect(Collectors.toList());
    }

}
