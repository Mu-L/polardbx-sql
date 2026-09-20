/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudftest;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.Pair;
import com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf.PatternRngUdfImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PatternRngUdfImplTest {

    @Test
    public void test1() {
//        PartitionByPattern autoPartition = new PartitionByPattern();
//        autoPartition.setPatternValue(256);
//        autoPartition.setDefaultNode(2);
//        autoPartition.setMapFile("dbleres/partition-pattern-test.txt");
//        autoPartition.init();

//# id partition range start-end ,data node index
//###### first host configuration
//        1-32=0
//        33-64=1
//        65-96=2
//        97-128=3
//######## second host configuration
//        129-160=4
//        161-192=5
//        193-224=6
//        225-256=7
//        0-0=7

        List<Pair<String, Integer>> partMappings = new ArrayList<>();
        partMappings.add(new Pair<>("1-32", 0));
        partMappings.add(new Pair<>("33-64", 1));
        partMappings.add(new Pair<>("65-96", 2));
        partMappings.add(new Pair<>("97-128", 3));
        partMappings.add(new Pair<>("129-160", 4));
        partMappings.add(new Pair<>("161-192", 5));
        partMappings.add(new Pair<>("193-224", 6));
        partMappings.add(new Pair<>("225-256", 7));
        partMappings.add(new Pair<>("0-0", 7));

        PatternRngUdfImpl autoPartition = new PatternRngUdfImpl(2, 256, partMappings);

        String idVal = "0";
        Assert.assertEquals(true, 7 == doCompute(autoPartition, idVal));
        idVal = "45a";
        Assert.assertEquals(true, 2 == doCompute(autoPartition, idVal));

//        Integer[] err1 = autoPartition.calculateRange("45a", "0");
//        Assert.assertEquals(true, 8 == err1.length);
//        Integer[] err2 = autoPartition.calculateRange("45", "0");
//        Assert.assertEquals(true, 0 == err2.length);
//
//        Integer[] normal = autoPartition.calculateRange("0", "45");
//        Assert.assertEquals(true, 3 == normal.length);
//
//        Integer[] type1 = autoPartition.calculateRange("1", "45");
//        Assert.assertEquals(true, 2 == type1.length);
//
//        Integer[] type2 = autoPartition.calculateRange("200", "260");
//        Assert.assertEquals(true, 3 == type2.length);
//
//        Integer[] type3 = autoPartition.calculateRange("200", "456");
//        Assert.assertEquals(true, 8 == type3.length);
    }

    @Test
    public void test3() {

        List<Pair<String, Integer>> partMappings = new ArrayList<>();
        partMappings.add(new Pair<>("1-32", 0));
        partMappings.add(new Pair<>("33-64", 1));
        partMappings.add(new Pair<>("65-96", 2));
        partMappings.add(new Pair<>("97-128", 3));
        partMappings.add(new Pair<>("129-160", 4));
        partMappings.add(new Pair<>("161-192", 5));
        partMappings.add(new Pair<>("193-224", 6));
        partMappings.add(new Pair<>("225-256", 7));
        partMappings.add(new Pair<>("0-0", 7));

        PatternRngUdfImpl autoPartition = new PatternRngUdfImpl(2, 256, partMappings);

        String idVal = "0";
        Assert.assertEquals(true, 7 == doCompute(autoPartition, idVal));
        idVal = "64";
        Assert.assertEquals(true, 1 == doCompute(autoPartition, idVal));
        idVal = "98";
        Assert.assertEquals(true, 3 == doCompute(autoPartition, idVal));
        idVal = "128";
        Assert.assertEquals(true, 3 == doCompute(autoPartition, idVal));
        idVal = "129";
        Assert.assertEquals(true, 4 == doCompute(autoPartition, idVal));
        idVal = "257";
        Assert.assertEquals(true, 0 == doCompute(autoPartition, idVal));
        idVal = "289";
        Assert.assertEquals(true, 1 == doCompute(autoPartition, idVal));

    }

    /*
        public void test2() {
		PartitionByPattern autoPartition = new PartitionByPattern();
		autoPartition.setPatternValue(256);
		autoPartition.setDefaultNode(2);
		autoPartition.setMapFile("partition-pattern1.txt");
		autoPartition.init();
		String idVal = "0";
		Assert.assertEquals(true, 0 == autoPartition.calculate(idVal));
		idVal = "45a";
		Assert.assertEquals(true, 2 == autoPartition.calculate(idVal));

		Integer [] err1 = autoPartition.calculateRange("45a", "0");
		Assert.assertEquals(true, 9 == err1.length);
		Integer [] err2 = autoPartition.calculateRange("45", "0");
		Assert.assertEquals(true, 0 == err2.length);

		Integer [] type1 = autoPartition.calculateRange("0", "45");
		Assert.assertEquals(true, 3 == type1.length);

		Integer [] type2 = autoPartition.calculateRange("200", "260");
		Assert.assertEquals(true, 4 == type2.length);

		Integer [] type3 = autoPartition.calculateRange("200", "456");
		Assert.assertEquals(true, 9 == type3.length);
	}

    */
    public static void main(String[] args) {
        PatternRngUdfImplTest test = new PatternRngUdfImplTest();
        test.test1();
        //test.test2();
        return;
    }

    private Integer doCompute(UserDefinedJavaFunction func, String str) {
        Object[] args = new Object[1];
        args[0] = str;
        Integer rs = (Integer) func.compute(args);
        return rs;
    }
}
