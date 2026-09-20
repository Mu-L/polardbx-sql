package com.alibaba.polardbx.qatest.ddl.auto.omc30.rebuildcleanup;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import org.junit.After;
import org.junit.Before;

abstract class RebuildCleanupTestBase extends DDLBaseNewDBTestCase {

    protected static final String TABLE_NAME = RebuildCleanupTestSupport.TABLE_NAME;

    protected abstract String databaseName();

    @Before
    public void setUpRebuildCleanup() {
        RebuildCleanupTestSupport.prepareTable(tddlConnection, databaseName());
    }

    @After
    public void tearDownRebuildCleanup() {
        RebuildCleanupTestSupport.dropDatabase(tddlConnection, databaseName());
    }

    protected String forceOmc30Hint() {
        return RebuildCleanupTestSupport.forceOmc30Hint();
    }

    protected String forceOmc30WithGsiHint() {
        return RebuildCleanupTestSupport.forceOmc30WithGsiHint();
    }

    protected int queryCount(String sql) {
        return RebuildCleanupTestSupport.queryCount(tddlConnection, sql);
    }
}
