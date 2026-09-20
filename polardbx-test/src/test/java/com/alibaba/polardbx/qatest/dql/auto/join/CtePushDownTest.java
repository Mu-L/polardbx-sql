package com.alibaba.polardbx.qatest.dql.auto.join;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class CtePushDownTest extends BaseTestCase {

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection conn = getPolardbxConnection0()) {
            conn.createStatement().execute("drop database if exists cte_push_down");
        }
    }

    @Test
    public void test() throws SQLException {
        String create1 = "create table if not exists  t1(id int, name varchar(20), primary key(id)) single";
        String create2 = "create table if not exists  t2(id int, name varchar(20), primary key(id)) single";
        String sql =
            "/*TDDL:a()*/with recursive t as (select * from t1 union all select * from t limit 5), o as (select * from t1 where"
                + " id in (select id from t) or name in (select name from t2) union all select * from t2 )"
                + " select * , row_number() over (partition by id order by name) from o";

        try (Connection conn = getPolardbxConnection0()) {
            conn.createStatement().execute("create database if not exists cte_push_down mode = auto");
            conn.createStatement().execute("use cte_push_down");
            conn.createStatement().execute(create1);
            conn.createStatement().execute(create2);
            conn.createStatement().executeQuery("explain " + sql);
        }
    }
}
