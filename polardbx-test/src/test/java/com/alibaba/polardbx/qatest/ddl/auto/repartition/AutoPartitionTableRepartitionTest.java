package com.alibaba.polardbx.qatest.ddl.auto.repartition;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AutoPartitionTableRepartitionTest extends DDLBaseNewDBTestCase {

    @Test
    public void testAutoPartitionTableRepartition() throws InterruptedException {
        // 创建默认主键拆分表
        String primaryTableName = "auto_tb123";
        String sql = "drop table if exists " + primaryTableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create table %s (a int primary key auto_increment, b int, c int, "
                + "unique key idx_b(b), key idx_c(c))", primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 插入初始数据
        sql = String.format("insert into %s (b, c) values (1, 1)", primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 启动背景流量，insert 写入重复数据，预期报错
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 60000) { // 运行60秒
                try {
                    String insertSql = String.format("insert into %s (b, c) values (1, 2)", primaryTableName);
                    JdbcUtil.executeUpdateFailed(tddlConnection, insertSql, "Duplicate entry");
                    Thread.sleep(100); // 稍微休眠一下，避免过于频繁的请求
                } catch (Exception e) {
                    // 忽略异常，继续执行
                }
            }
        });

        // 执行 repartition，成功变更不报错
        sql = String.format("alter table %s partition by key(b) partitions 4", primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 等待背景流量线程完成
        executor.shutdown();
        executor.awaitTermination(65, TimeUnit.SECONDS); // 等待最多65秒
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}