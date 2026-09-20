/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudftest;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf.PartEnumUdfImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;
import java.util.TreeMap;

public class PartEnumUdfImplTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void test1() {
//        PartitionByFileMap partition = new PartitionByFileMap();
//        partition.setMapFile("dbleres/partition-hash-int.txt");
//        partition.init();
        Map<String, Integer> mappings = new TreeMap<>();
        mappings.put("10000", 0);
        mappings.put("10010", 1);
        PartEnumUdfImpl partition = new PartEnumUdfImpl(0, -1, mappings);

        String idVal = "10000";
        Assert.assertEquals(true, 0 == doCompute(partition, idVal));
        idVal = "10010";
        Assert.assertEquals(true, 1 == doCompute(partition, idVal));
        idVal = "10020";
//        Assert.assertEquals(true, null == doCompute(partition, idVal));
        try {
            Assert.assertEquals(true, null == doCompute(partition, idVal));
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getMessage().contains("find any valid data node"));
        }

//        Map<String, String> map = partition.getAllProperties();
//        Assert.assertEquals(true, map.get("mapFile").equals("{\"10000\":\"0\"," +
//                "\"10010\":\"1\"}"));
    }

    @Test
    public void test2() {
//        PartitionByFileMap partition = new PartitionByFileMap();
//        partition.setMapFile("dbleres/partition-hash-int.txt");
//        partition.setDefaultNode(1);
//        partition.init();

        Map<String, Integer> mappings = new TreeMap<>();
        mappings.put("10000", 0);
        mappings.put("10010", 1);
        PartEnumUdfImpl partition = new PartEnumUdfImpl(0, 1, mappings);

        String idVal = "10020";
        Assert.assertEquals(true, 1 == doCompute(partition, idVal));
    }

    @Test
    public void test3() {
//        PartitionByFileMap partition = new PartitionByFileMap();
//        partition.setMapFile("dbleres/partition-hash-int.txt");
//        partition.setDefaultNode(1);
//        partition.init();

        Map<String, Integer> mappings = new TreeMap<>();
        mappings.put("10000", 0);
        mappings.put("10010", 1);
        PartEnumUdfImpl partition = new PartEnumUdfImpl(0, 1, mappings);

        String idVal = "xx";
        thrown.expect(IllegalArgumentException.class);
        doCompute(partition, idVal);
    }

    @Test
    public void test4() {
//        PartitionByFileMap partition = new PartitionByFileMap();
//        partition.setMapFile("dbleres/partition-hash-int2.txt");
//        partition.setType(-1);
//        partition.init();

        Map<String, Integer> mappings = new TreeMap<>();
        mappings.put("A", 0);
        mappings.put("B", 1);
        PartEnumUdfImpl partition = new PartEnumUdfImpl(-1, -1, mappings);

        String idVal = "A";
        Assert.assertEquals(true, 0 == doCompute(partition, idVal));
        idVal = "B";
        Assert.assertEquals(true, 1 == doCompute(partition, idVal));
        idVal = "C";

        try {
            Assert.assertEquals(true, null == doCompute(partition, idVal));
        } catch (Throwable ex) {
            Assert.assertTrue(ex.getMessage().contains("find any valid data node"));
        }
    }

    @Test
    public void test5() {
//        PartitionByFileMap partition = new PartitionByFileMap();
//        partition.setMapFile("dbleres/partition-hash-int2.txt");
//        partition.setDefaultNode(1);
//        partition.setType(-1);
//        partition.init();

        Map<String, Integer> mappings = new TreeMap<>();
        mappings.put("A", 0);
        mappings.put("B", 1);
        PartEnumUdfImpl partition = new PartEnumUdfImpl(-1, 1, mappings);

        String idVal = "C";
        Assert.assertEquals(true, 1 == doCompute(partition, idVal));
    }

    @Test
    public void test6() {
//        PartitionByFileMap partition = new PartitionByFileMap();
//        partition.setMapFile("dbleres/partition-hash-int2.txt");
//        partition.setDefaultNode(1);
//        partition.setType(-1);
//        partition.init();

        Map<String, Integer> mappings = new TreeMap<>();
        mappings.put("A", 0);
        mappings.put("B", 1);
        PartEnumUdfImpl partition = new PartEnumUdfImpl(-1, 1, mappings);

        String idVal = "1000";
        Assert.assertEquals(true, 1 == doCompute(partition, idVal));
    }

    private Integer doCompute(UserDefinedJavaFunction func, String str) {
        Object[] args = new Object[1];
        args[0] = str;
        Integer rs = (Integer) func.compute(args);
        return rs;
    }
}
