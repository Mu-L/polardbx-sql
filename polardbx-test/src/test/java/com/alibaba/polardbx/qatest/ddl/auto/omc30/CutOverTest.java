package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class CutOverTest extends DDLBaseNewDBTestCase {
    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testCutOverFailedByApplyTimeout() {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "omc_cut_over_failed_by_timeout";
        String sql =
            String.format("create table %s(a bigint primary key auto_increment, b int, c int) broadcast", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 启动一个 DML 线程，导入 10w 行数据
        Thread dmlThread = new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try (Connection conn = getPolardbxConnection()) {
                StringBuilder sb = new StringBuilder(String.format("insert into %s(b,c) values", tableName));
                for (int i = 0; i < 1024; i++) {
                    sb.append(String.format("(%s, %s)", i, i));
                    if (i != 1023) {
                        sb.append(",");
                    }
                }

                String insertSql = sb.toString();
                // load data
                for (int i = 0; i < 100; i++) {
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        dmlThread.start();

        String hint =
            "/*+TDDL:cmd_extra(OMC_MAX_RETRY_COUNT=1,FP_OMC_BEFORE_CUTOVER_SUSPEND=20000,OMC_CUTOVER_TIMEOUT=500)*/";
        sql = hint + String.format("alter table %s modify column b bigint, algorithm = omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testCutOverFailedByApplyTimeout2() {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "omc_cut_over_failed_by_timeout2";
        String sql =
            String.format("create table %s(a bigint primary key auto_increment, b int, c int) broadcast", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 启动一个 DML 线程，导入 10w 行数据
        Thread dmlThread = new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try (Connection conn = getPolardbxConnection()) {
                StringBuilder sb = new StringBuilder(String.format("insert into %s(b,c) values", tableName));
                for (int i = 0; i < 1024; i++) {
                    sb.append(String.format("(%s, %s)", i, i));
                    if (i != 1023) {
                        sb.append(",");
                    }
                }

                String insertSql = sb.toString();
                // load data
                for (int i = 0; i < 100; i++) {
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        dmlThread.start();

        String hint =
            "/*+TDDL:cmd_extra(OMC_MAX_RETRY_COUNT=2,FP_OMC_BEFORE_CUTOVER_SUSPEND=20000,OMC_CUTOVER_TIMEOUT=500)*/";
        sql = hint + String.format("alter table %s modify column b bigint, algorithm = omc", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }
}