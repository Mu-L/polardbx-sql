package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.join.LookupEquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LookupTableScanExecTest {

    @Mock
    private LogicalView logicalView;

    @Mock
    private ExecutionContext context;

    @Mock
    private TableScanClient scanClient;

    @Mock
    private SpillerFactory spillerFactory;

    @Mock
    private LookupPredicate predicate;

    @Mock
    private ParamManager paramManager;

    private LookupTableScanExec lookupTableScanExec;

    private List<LookupEquiJoinKey> allJoinKeys;

    private List<DataType> dataTypeList;

    private MockedStatic<ExecUtils> mockedExecUtils;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup mocks
        when(context.getParamManager()).thenReturn(paramManager);
        when(paramManager.getInt(ConnectionParams.LOOKUP_IN_VALUE_LIMIT)).thenReturn(1000);

        // Mock ExecUtils.isMppMode
        mockedExecUtils = mockStatic(ExecUtils.class);
        mockedExecUtils.when(() -> ExecUtils.isMppMode(any(ExecutionContext.class))).thenReturn(false);

        allJoinKeys = new ArrayList<>();
        dataTypeList = new ArrayList<>();

        lookupTableScanExec = new LookupTableScanExec(
            logicalView,
            context,
            scanClient,
            true, // shardEnabled
            spillerFactory,
            predicate,
            allJoinKeys,
            dataTypeList
        );
    }

    @After
    public void tearDown() {
        if (mockedExecUtils != null) {
            mockedExecUtils.close();
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testSwitchNoMgetSqlWhenScanClientNotReady() {
        // Given: scanClient has more splits (not ready)
        when(scanClient.noMoreSplit()).thenReturn(false);

        // When: call switchNoMgetSql
        // Then: should throw TddlRuntimeException
        lookupTableScanExec.switchNoMgetSql();
    }

    @Test
    public void testSwitchNoMgetSqlWithEmptyReservedSplits() {
        // Given: scanClient is ready and has splits
        when(scanClient.noMoreSplit()).thenReturn(true);

        JdbcSplit jdbcSplit = createMockJdbcSplit();
        Split split = new Split(false, jdbcSplit);
        List<Split> splitList = Lists.newArrayList(split);

        when(scanClient.getSplitList()).thenReturn(splitList);

        // When: call switchNoMgetSql for the first time (reservedSplits is null)
        lookupTableScanExec.switchNoMgetSql();

        // Then: should clear splitList and add new splits with sqlTemplateWithoutMget
        verify(scanClient, times(2)).getSplitList(); // called twice: once to backup, once to clear
        verify(scanClient, times(1)).addSplit(any(Split.class));
        assertTrue(splitList.isEmpty()); // splitList should be cleared
    }

    @Test
    public void testSwitchNoMgetSqlWithExistingReservedSplits() {
        // Given: setup initial state with reservedSplits
        when(scanClient.noMoreSplit()).thenReturn(true);

        JdbcSplit jdbcSplit = createMockJdbcSplit();
        Split split = new Split(false, jdbcSplit);
        List<Split> splitList = Lists.newArrayList(split);

        when(scanClient.getSplitList()).thenReturn(splitList);

        // First call to initialize reservedSplits
        lookupTableScanExec.switchNoMgetSql();

        // When: call switchNoMgetSql again (reservedSplits already exists)
        JdbcSplit anotherJdbcSplit = createMockJdbcSplit();
        Split anotherSplit = new Split(false, anotherJdbcSplit);
        splitList.add(anotherSplit);

        lookupTableScanExec.switchNoMgetSql();

        // Then: should still work correctly
        verify(scanClient, times(2)).addSplit(any(Split.class));
    }

    @Test
    public void testSwitchNoMgetSqlCreatesCorrectJdbcSplit() {
        // Given: scanClient is ready and has splits
        when(scanClient.noMoreSplit()).thenReturn(true);

        JdbcSplit originalJdbcSplit = createMockJdbcSplit();
        Split split = new Split(false, originalJdbcSplit);
        List<Split> splitList = Lists.newArrayList(split);

        when(scanClient.getSplitList()).thenReturn(splitList);

        // When: call switchNoMgetSql
        lookupTableScanExec.switchNoMgetSql();

        // Then: verify that the new JdbcSplit uses sqlTemplateWithoutMget
        verify(originalJdbcSplit, times(1)).getSqlTemplateWithoutMget(); // called twice in JdbcSplit constructor
        verify(scanClient).addSplit(any(Split.class));
    }

    @Test
    public void testSwitchNoMgetSqlWithMultipleSplits() {
        // Given: scanClient has multiple splits
        when(scanClient.noMoreSplit()).thenReturn(true);

        JdbcSplit jdbcSplit1 = createMockJdbcSplit();
        JdbcSplit jdbcSplit2 = createMockJdbcSplit();
        Split split1 = new Split(false, jdbcSplit1);
        Split split2 = new Split(false, jdbcSplit2);
        List<Split> splitList = Lists.newArrayList(split1, split2);

        when(scanClient.getSplitList()).thenReturn(splitList);

        lookupTableScanExec.switchNoMgetSql();

        verify(scanClient, times(2)).addSplit(any(Split.class));
        assertTrue(splitList.isEmpty()); // all splits should be cleared from original list
    }

    private JdbcSplit createMockJdbcSplit() {
        JdbcSplit jdbcSplit = mock(JdbcSplit.class);

        // Mock all the getters that are used in JdbcSplit constructor
        when(jdbcSplit.getCatalogName()).thenReturn("test_catalog");
        when(jdbcSplit.getSchemaName()).thenReturn("test_schema");
        when(jdbcSplit.getDbIndex()).thenReturn("test_db");
        when(jdbcSplit.getHint()).thenReturn("test_hint".getBytes());
        when(jdbcSplit.getSqlTemplate()).thenReturn(BytesSql.buildBytesSql("SELECT * FROM test"));
        when(jdbcSplit.getSqlTemplateWithoutMget()).thenReturn(
            BytesSql.buildBytesSql("SELECT * FROM test WITHOUT MGET"));
        when(jdbcSplit.getOrderBy()).thenReturn(null);
        when(jdbcSplit.getParams()).thenReturn(
            new ArrayList<List<com.alibaba.polardbx.common.jdbc.ParameterContext>>());
        when(jdbcSplit.getHostAddress()).thenReturn("localhost:3306");
        List<List<String>> tableNames = new ArrayList<>();
        tableNames.add(Lists.newArrayList("test_table"));
        when(jdbcSplit.getTableNames()).thenReturn(tableNames);
        when(jdbcSplit.getTransactionRw()).thenReturn(null); // ITransaction.RW is an enum, using null for simplicity
        when(jdbcSplit.isContainSelect()).thenReturn(true);
        when(jdbcSplit.getIntraGroupSortKey()).thenReturn(null);
        when(jdbcSplit.getGalaxyDigest()).thenReturn(null);
        when(jdbcSplit.isSupportGalaxyPrepare()).thenReturn(false);
        when(jdbcSplit.getSelect()).thenReturn(null);
        when(jdbcSplit.getStartSql()).thenReturn(null);

        return jdbcSplit;
    }

}
