/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudftest;


import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf.JumpStrUdfImpl;
import org.junit.Assert;
import org.junit.Test;

public class JumpStrUdfImplTest {

    @Test
    public void test1() {
//        PartitionByJumpConsistentHash partition = new PartitionByJumpConsistentHash();
//        partition.setHashSlice("0:0");
//        partition.setPartitionCount(5);
//        partition.init();
        JumpStrUdfImpl partition = new JumpStrUdfImpl(5, "0:0");
        Assert.assertEquals(1, (int) doCompute(partition, "8"));
        Assert.assertEquals(4, (int) doCompute(partition, "5"));
    }

    @Test
    public void test2() {
//        PartitionByJumpConsistentHash partition = new PartitionByJumpConsistentHash();
//        partition.setHashSlice("0:0");
//        partition.setPartitionCount(4);
//        partition.init();
        JumpStrUdfImpl partition = new JumpStrUdfImpl(4, "0:0");
        Assert.assertEquals(1, (int) doCompute(partition, "8"));
        Assert.assertEquals(0, (int) doCompute(partition, "5"));
    }

    private Integer doCompute(UserDefinedJavaFunction func, String str) {
        Object[] args = new Object[1];
        args[0] = str;
        Integer rs = (Integer) func.compute(args);
        return rs;
    }

}