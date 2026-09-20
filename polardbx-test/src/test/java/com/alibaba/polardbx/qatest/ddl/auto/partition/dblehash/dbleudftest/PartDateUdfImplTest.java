/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudftest;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf.PartDateUdfImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PartDateUdfImplTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void test1() {
//        PartitionByDate partition = new PartitionByDate();
//        partition.setDateFormat("yyyy-MM-dd");
//        partition.setsBeginDate("2014-01-01");
//        partition.setsPartionDay("10");
//        partition.init();

        String dateFormat = "yyyy-MM-dd";
        String sBeginDate = "2014-01-01";
        String sPartitionDay = "10";
        PartDateUdfImpl partition = new PartDateUdfImpl(-1, dateFormat, sBeginDate, null, sPartitionDay);

        Assert.assertEquals(true, 0 == doCompute(partition, "2014-01-01"));
        Assert.assertEquals(true, 0 == doCompute(partition, "2014-01-10"));
        Assert.assertEquals(true, 1 == doCompute(partition, "2014-01-11"));
        Assert.assertEquals(true, 12 == doCompute(partition, "2014-05-01"));

    }

    @Test
    public void test2() {
//        PartitionByDate partition = new PartitionByDate();
//        partition.setDateFormat("yyyy-MM-dd");
//        partition.setsBeginDate("2014-01-01");
//        partition.setsEndDate("2014-01-31");
//        partition.setsPartionDay("10");
//        partition.init();

        String dateFormat = "yyyy-MM-dd";
        String sBeginDate = "2014-01-01";
        String sEndDate = "2014-01-31";
        String sPartitionDay = "10";
        PartDateUdfImpl partition = new PartDateUdfImpl(-1, dateFormat, sBeginDate, sEndDate, sPartitionDay);

        /**
         * 0 : 01.01-01.10,02.10-02.19
         * 1 : 01.11-01.20,02.20-03.01
         * 2 : 01.21-01.30,03.02-03.12
         * 3  : 01.31-02-09,03.13-03.23
         */
        Assert.assertEquals(true, 0 == doCompute(partition, "2014-01-10"));
        Assert.assertEquals(true, 0 == doCompute(partition, "2014-01-10"));
        Assert.assertEquals(true, 1 == doCompute(partition, "2014-01-11"));
        Assert.assertEquals(true, 3 == doCompute(partition, "2014-01-31"));
        Assert.assertEquals(true, 3 == doCompute(partition, "2014-02-01"));
        Assert.assertEquals(true, 0 == doCompute(partition, "2014-02-19"));
        Assert.assertEquals(true, 1 == doCompute(partition, "2014-02-20"));
        Assert.assertEquals(true, 1 == doCompute(partition, "2014-03-01"));
        Assert.assertEquals(true, 2 == doCompute(partition, "2014-03-02"));
        Assert.assertEquals(true, 2 == doCompute(partition, "2014-03-11"));
        Assert.assertEquals(true, 3 == doCompute(partition, "2014-03-20"));
        Assert.assertEquals(true, 0 == doCompute(partition, "2014-03-24"));

    }

    @Test
    public void test3() {
//        PartitionByDate partition = new PartitionByDate();
//        partition.setDateFormat("yyyy-MM-dd");
//        partition.setsBeginDate("2014-01-01");
//        partition.setsEndDate("2014-01-30");
//        partition.setsPartionDay("10");
//        partition.init();

        String dateFormat = "yyyy-MM-dd";
        String sBeginDate = "2014-01-01";
        String sEndDate = "2014-01-30";
        String sPartitionDay = "10";
        PartDateUdfImpl partition = new PartDateUdfImpl(-1, dateFormat, sBeginDate, sEndDate, sPartitionDay);

        /**
         * 0 : 01.01-01.10,01.31-02.09
         * 1 : 01.11-01.20,02.10-02.19
         * 2 : 01.21-01.30,02.20-03.01
         */
        Assert.assertEquals(true, 0 == doCompute(partition, "2014-01-01"));
        Assert.assertEquals(true, 0 == doCompute(partition, "2014-01-10"));
        Assert.assertEquals(true, 1 == doCompute(partition, "2014-01-11"));
        Assert.assertEquals(true, 2 == doCompute(partition, "2014-01-30"));
        Assert.assertEquals(true, 0 == doCompute(partition, "2014-01-31"));
        Assert.assertEquals(true, 0 == doCompute(partition, "2014-02-09"));
        Assert.assertEquals(true, 1 == doCompute(partition, "2014-02-10"));
        Assert.assertEquals(true, 1 == doCompute(partition, "2014-02-19"));
        Assert.assertEquals(true, 2 == doCompute(partition, "2014-02-20"));
        Assert.assertEquals(true, 2 == doCompute(partition, "2014-03-01"));


    }

    @Test
    public void test4() {
//        PartitionByDate partition = new PartitionByDate();
//        partition.setDateFormat("yyyy-MM-dd");
//        partition.setsBeginDate("2014-01-01");
//        partition.setsEndDate("2014-01-30");
//        partition.setsPartionDay("10");
//        partition.init();

        String dateFormat = "yyyy-MM-dd";
        String sBeginDate = "2014-01-01";
        String sEndDate = "2014-01-30";
        String sPartitionDay = "10";
        PartDateUdfImpl partition = new PartDateUdfImpl(-1, dateFormat, sBeginDate, sEndDate, sPartitionDay);


        thrown.expect(IllegalArgumentException.class);
        doCompute(partition, "2014/01/01");
    }

    @Test
    public void test5() {

//        PartitionByDate partition = new PartitionByDate();
//        partition.setDateFormat("yyyy-MM-dd");
//        partition.setsBeginDate("2014-01-01");
//        partition.setsEndDate("2014-01-30");
//        partition.setsPartionDay("10");
//        partition.init();

        String dateFormat = "yyyy-MM-dd";
        String sBeginDate = "2014-01-01";
        String sEndDate = "2014-01-30";
        String sPartitionDay = "10";
        PartDateUdfImpl partition = new PartDateUdfImpl(-1, dateFormat, sBeginDate, sEndDate, sPartitionDay);

        Assert.assertEquals(true, 0 == doCompute(partition, "2014-01-01 12:00:03"));
    }

    @Test
    public void test6() {
//        PartitionByDate partition = new PartitionByDate();
//
//        partition.setDateFormat("yyyy-MM-dd");
//        partition.setsBeginDate("2014-01-01");
//        partition.setsEndDate("2014-01-30");
//        partition.setsPartionDay("10");
//        partition.init();

        String dateFormat = "yyyy-MM-dd";
        String sBeginDate = "2014-01-01";
        String sEndDate = "2014-01-30";
        String sPartitionDay = "10";
        PartDateUdfImpl partition = new PartDateUdfImpl(-1, dateFormat, sBeginDate, sEndDate, sPartitionDay);

        try {
            doCompute(partition, "2012-12-31");
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getMessage().contains("find any valid data node"));
        }
//        Assert.assertEquals(true, null == doCompute(partition, "2012-12-31"));
    }

    @Test
    public void test7() {
//        PartitionByDate partition = new PartitionByDate();
//        partition.setDateFormat("yyyy-MM-dd");
//        partition.setsBeginDate("2014-01-01");
//        partition.setsEndDate("2014-01-30");
//        partition.setsPartionDay("10");
//        partition.setDefaultNode(0);
//        partition.init();

        String dateFormat = "yyyy-MM-dd";
        String sBeginDate = "2014-01-01";
        String sEndDate = "2014-01-30";
        String sPartitionDay = "10";
        PartDateUdfImpl partition = new PartDateUdfImpl(0, dateFormat, sBeginDate, sEndDate, sPartitionDay);


        Assert.assertEquals(true, 0 == doCompute(partition, "2012-12-31"));
    }

    @Test
    public void test8() {
//        PartitionByDate partition = new PartitionByDate();
//        partition.setDateFormat("yyyy-MM-dd");
//        partition.setsBeginDate("2014-01-01");
//        partition.setsPartionDay("10");
//        partition.setDefaultNode(0);
//        partition.init();

        String dateFormat = "yyyy-MM-dd";
        String sBeginDate = "2014-01-01";
        String sPartitionDay = "10";
        PartDateUdfImpl partition = new PartDateUdfImpl(0, dateFormat, sBeginDate, null, sPartitionDay);

        Assert.assertEquals(true, 0 == doCompute(partition, "2012-12-31"));
    }

    private Integer doCompute(UserDefinedJavaFunction func, String str) {
        Object[] args = new Object[1];
        args[0] = str;
        Integer rs = (Integer) func.compute(args);
        return rs;
    }
}