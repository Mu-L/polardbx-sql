/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudftest;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf.PartLongUdfImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PartLongUdfImplTest {
    private Integer[] allNode = new Integer[0];
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testCalculate1() {
        PartLongUdfImpl
            rule = new PartLongUdfImpl("2", "512");
        String value = "0";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
        value = "511";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
        value = "512";
        Assert.assertEquals(true, 1 == doCompute(rule, value));
        value = "1023";
        Assert.assertEquals(true, 1 == doCompute(rule, value));
        value = "1024";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
    }

    @Test
    public void testCalculate2() {
        PartLongUdfImpl
            rule = new PartLongUdfImpl("2", "1");
        String value = "0";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
        value = "1";
        Assert.assertEquals(true, 1 == doCompute(rule, value));
        value = "2";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
        value = "-1";
        Assert.assertEquals(true, 1 == doCompute(rule, value));
        value = "-2";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
    }

    @Test
    public void testCalculate3() {
        PartLongUdfImpl
            rule = new PartLongUdfImpl("3", "1");
        String value = "0";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
        value = "1";
        Assert.assertEquals(true, 1 == doCompute(rule, value));
        value = "2";
        Assert.assertEquals(true, 2 == doCompute(rule, value));
        value = "3";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
        value = "-1";
        Assert.assertEquals(true, 2 == doCompute(rule, value));
        value = "-2";
        Assert.assertEquals(true, 1 == doCompute(rule, value));
        value = "-3";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
    }

    @Test
    public void testCalculate4() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("1,2");
//        rule.setPartitionLength("1,2");
//        rule.init();
        PartLongUdfImpl rule = new PartLongUdfImpl("1,2", "1,2");
        String value = "0";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
        value = "1";
        Assert.assertEquals(true, 1 == doCompute(rule, value));
        value = "2";
        Assert.assertEquals(true, 1 == doCompute(rule, value));
        value = "3";
        Assert.assertEquals(true, 2 == doCompute(rule, value));
        value = "4";
        Assert.assertEquals(true, 2 == doCompute(rule, value));
        value = "5";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
    }

//    @Test
//    public void testCalculate5() {
//        Partitionbylongudf rule = new Partitionbylongudf("3", "1440");
//        thrown.expect(RuntimeException.class);
//    }

    @Test
    public void testCalculate6() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("3");
//        rule.setPartitionLength("1");
//        rule.init();
        PartLongUdfImpl rule = new PartLongUdfImpl("3", "1");

        String value = "";
        thrown.expect(IllegalArgumentException.class);
        doCompute(rule, value);
    }

    @Test
    public void testCalculate7() {
        thrown.expect(RuntimeException.class);
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("-2");
//        rule.setPartitionLength("1");
//        rule.init();
        PartLongUdfImpl rule = new PartLongUdfImpl("-2", "1");
//        doCompute(rule, value);

    }

    @Test
    public void testCalculate8() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("0");
//        rule.setPartitionLength("1");
//        rule.init();

        thrown.expect(RuntimeException.class);
        PartLongUdfImpl rule = new PartLongUdfImpl("0", "1");

    }

    @Test
    public void testCalculate9() {
        thrown.expect(RuntimeException.class);

//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("1,-2");
//        rule.setPartitionLength("1,4");
//        rule.init();

        PartLongUdfImpl rule = new PartLongUdfImpl("1,-2", "1,4");

    }

    @Test
    public void testCalculateRange() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("2");
//        rule.setPartitionLength("512");
//        rule.init();
        PartLongUdfImpl rule = new PartLongUdfImpl("2", "512");
//        String bgeinValue = "0";
//        String endValue = "512";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{0, 1};
//        checkCalculateRange(expect, fact);

    }

    @Test
    public void testCalculateRange2() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("2");
//        rule.setPartitionLength("512");
//        rule.init();
//        String bgeinValue = "0";
//        String endValue = "1024";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        checkCalculateRange(allNode, fact);
    }

    @Test
    public void testCalculateRange3() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("2");
//        rule.setPartitionLength("512");
//        rule.init();
//        String bgeinValue = "1";
//        String endValue = "1024";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{0, 1};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange4() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("2");
//        rule.setPartitionLength("512");
//        rule.init();
//        String bgeinValue = "1";
//        String endValue = "2048";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        checkCalculateRange(allNode, fact);
    }

    @Test
    public void testCalculateRange5() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("2");
//        rule.setPartitionLength("512");
//        rule.init();
//        String bgeinValue = "514";
//        String endValue = "1024";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{1, 0};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange6() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("4");
//        rule.setPartitionLength("256");
//        rule.init();
//        String bgeinValue = "769";
//        String endValue = "1274";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{3, 0};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange7() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("8");
//        rule.setPartitionLength("128");
//        rule.init();
//        String bgeinValue = "769";
//        String endValue = "1274";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{6, 7, 0, 1};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange8() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("8");
//        rule.setPartitionLength("128");
//        rule.init();
//        String bgeinValue = "770";
//        String endValue = "1793";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{6, 7, 0, 1, 2, 3, 4, 5};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange9() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("8");
//        rule.setPartitionLength("128");
//        rule.init();
//        String bgeinValue = "770";
//        String endValue = "771";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{6};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange10() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("8");
//        rule.setPartitionLength("128");
//        rule.init();
//        String bgeinValue = "769";
//        String endValue = "895";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{6};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange11() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("8");
//        rule.setPartitionLength("128");
//        rule.init();
//        String bgeinValue = "769";
//        String endValue = "1793";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        checkCalculateRange(allNode, fact);
    }

    @Test
    public void testCalculateRange12() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("2");
//        rule.setPartitionLength("512");
//        rule.init();
//        String bgeinValue = "-2";
//        String endValue = "-1";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{1};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange13() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("2");
//        rule.setPartitionLength("512");
//        rule.init();
//        String bgeinValue = "-1026";
//        String endValue = "-3";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{1, 0};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange14() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("2");
//        rule.setPartitionLength("512");
//        rule.init();
//        String bgeinValue = "1021";
//        String endValue = "1023";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{1};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange15() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("2");
//        rule.setPartitionLength("512");
//        rule.init();
//        String bgeinValue = "1021";
//        String endValue = "2043";
//        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
//        Integer[] expect = new Integer[]{1, 0};
//        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculate16() {
//        PartitionByLong rule = new PartitionByLong();
//        rule.setPartitionCount("8");
//        rule.setPartitionLength("360");
        PartLongUdfImpl
            rule = new PartLongUdfImpl("8", "360");
        String value = "0";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
        value = "359";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
        value = "360";
        Assert.assertEquals(true, 1 == doCompute(rule, value));
        value = "719";
        Assert.assertEquals(true, 1 == doCompute(rule, value));
        value = "720";
        Assert.assertEquals(true, 2 == doCompute(rule, value));
        value = "2879";
        Assert.assertEquals(true, 7 == doCompute(rule, value));
        value = "2880";
        Assert.assertEquals(true, 0 == doCompute(rule, value));
    }

    private Integer doCompute(UserDefinedJavaFunction func, String str) {
        Object[] args = new Object[1];
        args[0] = str;
        Integer rs = (Integer) func.compute(args);
        return rs;
    }

    private void checkCalculateRange(Integer[] expect, Integer[] fact) {
//        Assert.assertEquals(true, expect.length == fact.length);
//        for (int i = 0; i < expect.length; i++) {
//            Assert.assertEquals(true, expect[i].intValue() == fact[i].intValue());
//        }
    }
}
