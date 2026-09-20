package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.FullMasterStatus;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.sql.SqlShowBinaryStreams;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.List;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogicalShowBinaryStreamsHandlerTest {

    @Test
    public void testHandle() throws Exception {
        try (MockedStatic<MetaDbUtil> mockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<CdcRpcClient> mockedStatic1 = Mockito.mockStatic(CdcRpcClient.class)) {

            IRepository repository = mock(IRepository.class);
            LogicalShow relNode = mock(LogicalShow.class);
            ExecutionContext executionContext = mock(ExecutionContext.class);
            SqlShowBinaryStreams sqlShowBinaryStreams = mock(SqlShowBinaryStreams.class);
            Connection connection = mock(Connection.class);

            List recordList = Lists.newArrayList();
            BinlogStreamRecord binlogStreamRecord = mock(BinlogStreamRecord.class);
            when(binlogStreamRecord.getStreamName()).thenReturn("s1");
            recordList.add(binlogStreamRecord);

            when(relNode.getNativeSqlNode()).thenReturn(sqlShowBinaryStreams);
            when(MetaDbUtil.getConnection()).thenReturn(connection);
            when(MetaDbUtil.query(anyString(), any(), any(Connection.class))).thenReturn(recordList);

            LogicalShowBinaryStreamsHandler handler = new LogicalShowBinaryStreamsHandler(repository);
            ArrayResultCursor cursor = (ArrayResultCursor) handler.handle(relNode, executionContext);
            Assert.assertEquals("SHOW BINARY STREAMS", cursor.getTableName());
            Assert.assertEquals(1, cursor.getRows().size());

            CdcRpcClient cdcRpcClient = mock(CdcRpcClient.class);
            CdcServiceGrpc.CdcServiceBlockingStub cdcServiceBlockingStub =
                mock(CdcServiceGrpc.CdcServiceBlockingStub.class);
            FullMasterStatus fullMasterStatus = mock(FullMasterStatus.class);
            when(sqlShowBinaryStreams.isFull()).thenReturn(true);
            when(CdcRpcClient.getCdcRpcClient()).thenReturn(cdcRpcClient);
            when(cdcRpcClient.getCdcServiceBlockingStub(any())).thenReturn(cdcServiceBlockingStub);
            when(cdcServiceBlockingStub.showFullMasterStatus(any())).thenReturn(fullMasterStatus);

            cursor = (ArrayResultCursor) handler.handle(relNode, executionContext);
            Assert.assertEquals("SHOW FULL BINARY STREAMS", cursor.getTableName());
            Assert.assertEquals(1, cursor.getRows().size());
        }
    }

    @Test
    public void testSqlParse() {
        String sql = "show full binary streams";
        SQLStatement statement = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES)
            .parseStatementList().get(0);
        Assert.assertEquals("SHOW FULL BINARY STREAMS", statement.toString());

        sql = "show binary streams";
        statement = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES)
            .parseStatementList().get(0);
        Assert.assertEquals("SHOW BINARY STREAMS", statement.toString());
    }
}
