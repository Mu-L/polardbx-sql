package com.alibaba.polardbx.executor.mpp.client;

import com.alibaba.polardbx.executor.mpp.execution.QueryInfo;
import com.alibaba.polardbx.executor.mpp.execution.QueryStats;
import com.alibaba.polardbx.executor.mpp.execution.StageInfo;
import com.alibaba.polardbx.executor.mpp.server.StatementResource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.Future;

public class MppResultCursorTest {

    @Test
    public void testDoClose() throws Exception {

        try (MockedStatic<StatementResource.Query> statementResourceMockedStatic = Mockito.mockStatic(
            StatementResource.Query.class)) {

            LocalStatementClient client = Mockito.mock(LocalStatementClient.class);
            ExecutionContext executionContext = Mockito.mock(ExecutionContext.class);
            CursorMeta cursorMeta = Mockito.mock(CursorMeta.class);
            RuntimeStatistics runtimeStatistics = Mockito.mock(RuntimeStatistics.class);

            Mockito.when(executionContext.getRuntimeStatistics()).thenReturn(runtimeStatistics);

            Future<QueryInfo> blockedQueryInfo = Mockito.mock(Future.class);
            Mockito.when(blockedQueryInfo.isDone()).thenReturn(false).thenReturn(true);
            Mockito.when(client.getBlockedQueryInfo()).thenReturn(blockedQueryInfo);

            QueryInfo queryInfo = Mockito.mock(QueryInfo.class);
            Mockito.when(blockedQueryInfo.get()).thenReturn(queryInfo);
            statementResourceMockedStatic.when(() -> StatementResource.Query.toQueryError(queryInfo)).thenReturn(null);

            StageInfo stageInfo = Mockito.mock(StageInfo.class);
            Mockito.when(queryInfo.getOutputStage()).thenReturn(Optional.of(stageInfo));

            QueryStats queryStats = Mockito.mock(QueryStats.class);
            Mockito.when(queryInfo.getQueryStats()).thenReturn(queryStats);
            Mockito.when(queryStats.getOperatorSummaries()).thenReturn(ImmutableList.of());

            MppResultCursor mppResultCursor = new MppResultCursor(client, executionContext, cursorMeta);

            mppResultCursor.waitQueryInfo(true, 100);
            mppResultCursor.doClose(ImmutableList.of());

        }

    }

}