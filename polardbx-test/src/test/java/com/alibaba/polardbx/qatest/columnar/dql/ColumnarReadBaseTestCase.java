package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.SQLException;

public class ColumnarReadBaseTestCase extends ExternalizedColumnTestBase {

    public static final String DB_NAME = "columnar_test";

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.createPartDatabase(tddlConnection, DB_NAME);
        }
    }

    @Before
    final public void before() {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
    }

    public static Connection getColumnarConnection() throws SQLException {
        Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection();
        JdbcUtil.useDb(connection, DB_NAME);
        return connection;
    }

    /**
     * Returns a fresh URL-bound workload-neutral connection on the given database.
     */
    public static Connection getColumnarConnection(String dbName) throws SQLException {
        return ConnectionManager.getInstance().newPolarDBXConnection(dbName);
    }

    /**
     * Returns a fresh connection forced into TP (row-store) workload.
     * <p>
     * Lab environments default to {@code WORKLOAD_TYPE=AP} globally, which routes
     * SELECT to the columnar path; for ext-col tests that need read-after-write
     * consistency on the row-store path, use this helper instead of
     * {@link #getColumnarConnection()} (which is intentionally workload-neutral).
     */
    public static Connection getTpConnection() throws SQLException {
        Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection();
        JdbcUtil.useDb(connection, DB_NAME);
        JdbcUtil.executeSuccess(connection, "SET SESSION WORKLOAD_TYPE=TP");
        return connection;
    }

}
