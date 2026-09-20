/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudftest;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf.PartStrUdfImpl;
import org.junit.Assert;
import org.junit.Test;

public class PartStrUdfImplTest {

    @Test
    public void test() {
        PartStrUdfImpl rule = null;

        //last 4
        rule = new PartStrUdfImpl("2", "512", "-4:0");
        //last 4 characters
        String idVal = "aaaabbb0000";
        Assert.assertEquals(true, 0 == doCompute(rule, idVal));
        idVal = "aaaabbb2359";
        Assert.assertEquals(true, 0 == doCompute(rule, idVal));
    }

    private Integer doCompute(UserDefinedJavaFunction func, String str) {
        Object[] args = new Object[1];
        args[0] = str;
        Integer rs = (Integer) func.compute(args);
        return rs;
    }
}