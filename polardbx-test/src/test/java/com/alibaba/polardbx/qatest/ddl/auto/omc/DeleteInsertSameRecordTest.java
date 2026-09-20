package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DeleteInsertSameRecordTest extends DDLBaseNewDBTestCase {

    static final int TOTAL_PK = 30_000;
    static final int RANGE = 30;
    static final int LOOP_COUNT = 2000;

    private Boolean useOmc30 = false;

    @Parameterized.Parameters(name = "{index}:useOmc30={0}")
    public static List<Object[]> prepareDate() {
        return ImmutableList.of(new Object[] {Boolean.FALSE}, new Object[] {Boolean.TRUE});
    }

    public DeleteInsertSameRecordTest(Boolean useOmc30) {
        this.useOmc30 = useOmc30;
    }

    @Before
    public void beforeMethod() {
        if (useOmc30) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_OMC_30 = true");
            JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        } else {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_OMC_30 = false");
        }
    }

    @Test
    public void testDeleteInsertWithOmc() throws InterruptedException {
        String createTableSql = "CREATE TABLE `del_ins_omc` ( `pk` int NOT NULL, `b` int, PRIMARY KEY (`pk`)) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // prepare data
        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            String sql = "INSERT INTO del_ins_omc (pk, b) VALUES (?, 1)";
            PreparedStatement insertStmt = conn.prepareStatement(sql);
            for (int i = 1; i <= TOTAL_PK; i++) {
                insertStmt.setInt(1, i);
                insertStmt.addBatch();

                // 每满一个 batch 执行一次
                if (i % 1000 == 0 || i == TOTAL_PK) {
                    insertStmt.executeBatch();
                    conn.commit();
                    insertStmt.clearBatch();
                    System.out.println("Inserted " + i + " rows");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        ExecutorService executor = Executors.newFixedThreadPool(4);

        // 删除线程1
        executor.submit(() -> {
            final Random random = new Random();
            try (Connection conn = getPolardbxConnection()) {
                conn.setAutoCommit(true);
                String sql = "DELETE FROM del_ins_omc WHERE pk = ?";
                PreparedStatement deleteStmt = conn.prepareStatement(sql);
                for (int i = 1; i <= LOOP_COUNT; i++) {
                    try {
                        deleteStmt.setInt(1, random.nextInt() % RANGE + 1);
                        deleteStmt.executeUpdate();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        // 删除线程2
        executor.submit(() -> {
            final Random random = new Random();
            try (Connection conn = getPolardbxConnection()) {
                conn.setAutoCommit(true);
                String sql = "DELETE FROM del_ins_omc WHERE pk = ?";
                PreparedStatement deleteStmt = conn.prepareStatement(sql);
                for (int i = 1; i <= LOOP_COUNT; i++) {
                    try {
                        deleteStmt.setInt(1, random.nextInt() % RANGE + 1);
                        deleteStmt.executeUpdate();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        // 插入线程1
        executor.submit(() -> {
            final Random random = new Random();
            try (Connection conn = getPolardbxConnection()) {
                conn.setAutoCommit(true);
                String sql = "INSERT IGNORE INTO del_ins_omc (pk, b) VALUES (?, 0)";
                PreparedStatement insertStmt = conn.prepareStatement(sql);
                for (int i = 1; i <= LOOP_COUNT; i++) {
                    try {
                        insertStmt.setInt(1, random.nextInt() % RANGE + 1);
                        insertStmt.executeUpdate();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        // 插入线程2
        executor.submit(() -> {
            final Random random = new Random();
            try (Connection conn = getPolardbxConnection()) {
                conn.setAutoCommit(true);
                String sql = "INSERT IGNORE INTO del_ins_omc (pk, b) VALUES (?, 0)";
                PreparedStatement insertStmt = conn.prepareStatement(sql);
                for (int i = 1; i <= LOOP_COUNT; i++) {
                    try {
                        insertStmt.setInt(1, random.nextInt() % RANGE + 1);
                        insertStmt.executeUpdate();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        String hint = "/*+TDDL:cmd_extra(FP_APPLY_DELETE_SUSPEND=5000)*/ ";
        try {
            System.out.println("omc start 1");
            String omcSql = hint + "ALTER TABLE del_ins_omc modify column b bigint, algorithm=OMC";
            JdbcUtil.executeUpdateSuccess(tddlConnection, omcSql);
            System.out.println("omc finished 1");

            System.out.println("omc start 2");
            omcSql = hint + "ALTER TABLE del_ins_omc modify column b int, algorithm=OMC";
            JdbcUtil.executeUpdateSuccess(tddlConnection, omcSql);
            System.out.println("omc finished 2");
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);
            System.out.println("executor shutdown.");
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table del_ins_omc");
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}