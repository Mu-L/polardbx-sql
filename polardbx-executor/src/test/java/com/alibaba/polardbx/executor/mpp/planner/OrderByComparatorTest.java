package com.alibaba.polardbx.executor.mpp.planner;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class OrderByComparatorTest {
    @Test
    public void test() {
        List<OrderByItem> list1 = new ArrayList<>();
        list1.add(new OrderByItem("col1", true));
        list1.add(new OrderByItem("col2", false));
        list1.add(new OrderByItem("col3", true));
        // ASC example
        List<OrderByItem> list2 = new ArrayList<>();
        list2.add(new OrderByItem("col1", true));
        list2.add(new OrderByItem("col2", false));
        list2.add(new OrderByItem("col3", true));
        // DESC example
        List<OrderByItem> list3 = new ArrayList<>();
        list3.add(new OrderByItem("col1", false));
        list3.add(new OrderByItem("col2", true));
        list3.add(new OrderByItem("col3", false));
        // PREFIX_ASC example
        List<OrderByItem> list4 = new ArrayList<>();
        list4.add(new OrderByItem("col1", true));
        list4.add(new OrderByItem("col2", false));
        // PREFIX_DESC example
        List<OrderByItem> list5 = new ArrayList<>();
        list5.add(new OrderByItem("col1", true));
        list5.add(new OrderByItem("col2", false));
        // NONE example
        List<OrderByItem> list6 = new ArrayList<>();
        list6.add(new OrderByItem("col1", false));
        list6.add(new OrderByItem("col4", true));
        // Test ASC, list1 vs list2
        Assert.assertEquals("(ASC, [col1 ASC, col2 DESC, col3 ASC])",
            OrderByComparator.compareOrderByLists(list1, list2).toString()); // ASC
        // Test DESC, list1 vs list3:
        Assert.assertEquals("(DESC, [col1 ASC, col2 DESC, col3 ASC])",
            OrderByComparator.compareOrderByLists(list1, list3).toString()); // DESC
        // Test PREFIX_ASC, list4 vs list1:
        Assert.assertEquals("(PREFIX_ASC, [col1 ASC, col2 DESC])",
            OrderByComparator.compareOrderByLists(list4, list1).toString()); // PREFIX_ASC
        // Test PREFIX_DESC, list5 vs list3
        Assert.assertEquals("(PREFIX_DESC, [col1 ASC, col2 DESC])",
            OrderByComparator.compareOrderByLists(list5, list3).toString()); // PREFIX_DESC
        //list1 vs list6
        Assert.assertEquals("(PREFIX_DESC, [col1 ASC])", OrderByComparator.compareOrderByLists(list1, list6).toString()); // NONE
    }

    @Test
    public void test1() {
        // top-n
        List<OrderByItem> list1 = new ArrayList<>();
        list1.add(new OrderByItem("col1", true));
        list1.add(new OrderByItem("col2", false));

        // sort key
        List<OrderByItem> list2 = new ArrayList<>();
        list2.add(new OrderByItem("col1", true));


        Assert.assertEquals("(PREFIX_ASC, [col1 ASC])",
            OrderByComparator.compareOrderByLists(list1, list2).toString()); // PREFIX_ASC
    }

    @Test
    public void test2() {
        // top-n
        List<OrderByItem> list1 = new ArrayList<>();
        list1.add(new OrderByItem("col1", false));
        list1.add(new OrderByItem("col2", false));

        // sort key
        List<OrderByItem> list2 = new ArrayList<>();
        list2.add(new OrderByItem("col1", true));


        Assert.assertEquals("(PREFIX_DESC, [col1 DESC])",
            OrderByComparator.compareOrderByLists(list1, list2).toString()); // PREFIX_DESC
    }
}