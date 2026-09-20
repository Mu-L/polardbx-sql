package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPurgeBinaryStream;
import org.apache.calcite.sql.parser.SqlParserPos;
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

public class LogicalPurgeBinaryStreamHandlerTest {

    @Test
    public void testHandle() throws Exception {
        try (MockedStatic<MetaDbUtil> mockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<CdcRpcClient> mockedStatic1 = Mockito.mockStatic(CdcRpcClient.class)) {
            IRepository repository = mock(IRepository.class);
            LogicalShow relNode = mock(LogicalShow.class);
            SqlPurgeBinaryStream sqlPurgeBinaryStream = mock(SqlPurgeBinaryStream.class);
            ExecutionContext executionContext = mock(ExecutionContext.class);
            Connection connection = mock(Connection.class);

            when(MetaDbUtil.getConnection()).thenReturn(connection);
            when(relNode.getNativeSqlNode()).thenReturn(sqlPurgeBinaryStream);
            when(sqlPurgeBinaryStream.getStreamName()).thenReturn(new SqlIdentifier("test", SqlParserPos.ZERO));
            LogicalPurgeBinaryStreamHandler handler = new LogicalPurgeBinaryStreamHandler(repository);

            try {
                when(MetaDbUtil.query(anyString(), any(), any(), any(Connection.class))).thenReturn(
                    Lists.newArrayList());
                handler.handle(relNode, executionContext);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals("binlog stream is not found", e.getMessage());
            }

            List recordList = Lists.newArrayList();
            BinlogStreamRecord binlogStreamRecord = mock(BinlogStreamRecord.class);
            when(binlogStreamRecord.getStreamName()).thenReturn("s1");
            when(binlogStreamRecord.getStatus()).thenReturn(1);
            recordList.add(binlogStreamRecord);
            when(MetaDbUtil.query(anyString(), any(), any(), any(Connection.class))).thenReturn(recordList);

            handler.handle(relNode, executionContext);

            try {
                when(binlogStreamRecord.getStatus()).thenReturn(0);
                handler.handle(relNode, executionContext);
            } catch (Exception e) {
                Assert.assertEquals("binlog stream status is not in Pending, can`t purge", e.getMessage());
            }
        }
    }

    @Test
    public void testSqlParse() {
        String sql = "purge binary stream s1";
        SQLStatement statement = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES)
            .parseStatementList().get(0);
        Assert.assertEquals("PURGE BINARY STREAM s1", statement.toString());
    }
}
