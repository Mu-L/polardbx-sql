/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudftest;


import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.Pair;
import com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf.NumRngUdfImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class NumRngUdfImplTest {

    @Test
    public void test1() {
//        AutoPartitionByLong autoPartition = new AutoPartitionByLong();
//        autoPartition.setMapFile("dbleres/autopartition-long.txt");
//        autoPartition.init();


//        0-200M=0
//        200M1-400M=1
//        400M1-600M=2

        List<Pair<String, Integer>> partMappings = new ArrayList<>();
        partMappings.add(new Pair<>("0-200M", 0));
        partMappings.add(new Pair<>("200M1-400M", 1));
        partMappings.add(new Pair<>("400M1-600M", 2));
        NumRngUdfImpl autoPartition = new NumRngUdfImpl(-1, partMappings);

        String idVal = "0";
        Assert.assertEquals(true, 0 == doCompute(autoPartition, idVal));

        idVal = "2000000";
        Assert.assertEquals(true, 0 == doCompute(autoPartition, idVal));

        idVal = "2000001";
        Assert.assertEquals(true, 1 == doCompute(autoPartition, idVal));

        idVal = "4000000";
        Assert.assertEquals(true, 1 == doCompute(autoPartition, idVal));

        idVal = "4000001";
        Assert.assertEquals(true, 2 == doCompute(autoPartition, idVal));
        idVal = "6000000";
        Assert.assertEquals(true, 2 == doCompute(autoPartition, idVal));
        idVal = "6000001";
//        Assert.assertEquals(true, null == doCompute(autoPartition, idVal));

        try {
        Assert.assertEquals(true, null == doCompute(autoPartition, idVal));
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getMessage().contains("find any valid data node"));
        }

//        Map<String, String> map = autoPartition.getAllProperties();
//        Assert.assertEquals(true, map.get("mapFile").equals("{\"0-200M\":\"0\"," +
//                "\"200M1-400M\":\"1\"," +
//                "\"400M1-600M\":\"2\"}"));
    }

    @Test
    public void test2() {
//        AutoPartitionByLong autoPartition = new AutoPartitionByLong();
//        autoPartition.setMapFile("dbleres/autopartition-long.txt");
//        autoPartition.setDefaultNode(0);
//        autoPartition.init();
        List<Pair<String, Integer>> partMappings = new ArrayList<>();
        partMappings.add(new Pair<>("0-200M", 0));
        partMappings.add(new Pair<>("200M1-400M", 1));
        partMappings.add(new Pair<>("400M1-600M", 2));
        NumRngUdfImpl autoPartition = new NumRngUdfImpl(0, partMappings);

        String idVal = "6000001";
        Assert.assertEquals(true, 0 == doCompute(autoPartition, idVal));
    }

    @Test
    public void test3() {
//        AutoPartitionByLong autoPartition = new AutoPartitionByLong();
//        autoPartition.setMapFile("dbleres/autopartition-long.txt");
//        autoPartition.setDefaultNode(0);
//        autoPartition.init();

        List<Pair<String, Integer>> partMappings = new ArrayList<>();
        partMappings.add(new Pair<>("0-200M", 0));
        partMappings.add(new Pair<>("200M1-400M", 1));
        partMappings.add(new Pair<>("400M1-600M", 2));
        NumRngUdfImpl autoPartition = new NumRngUdfImpl(0, partMappings);

//        Integer[] res = autoPartition.calculateRange("-1", "9999999999");
//        Assert.assertEquals(3, res.length);
//
//        res = autoPartition.calculateRange("-1", "10000000");
//        Assert.assertEquals(3, res.length);
//
//        res = autoPartition.calculateRange("-1", "100");
//        Assert.assertEquals(3, res.length);
//
//        res = autoPartition.calculateRange("0", "100");
//        Assert.assertEquals(1, res.length);
//
//        res = autoPartition.calculateRange("0", "100000");
//        Assert.assertEquals(1, res.length);
//
//
//        res = autoPartition.calculateRange("2000009", "3999999");
//        Assert.assertEquals(1, res.length);
//
//        res = autoPartition.calculateRange("2000009", "5999999");
//        Assert.assertEquals(2, res.length);
//
//
//        res = autoPartition.calculateRange("2000009", "59999999");
//        Assert.assertEquals(3, res.length);

    }

    private Integer doCompute(UserDefinedJavaFunction func, String str) {
        Object[] args = new Object[1];
        args[0] = str;
        Integer rs = (Integer) func.compute(args);
        return rs;
    }

}