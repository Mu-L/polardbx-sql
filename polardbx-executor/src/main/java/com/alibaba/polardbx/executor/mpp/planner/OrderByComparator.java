package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.utils.Pair;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

// Comparator class
public class OrderByComparator {
    public static Pair<OrderRelation, List<OrderByItem>> compareOrderByLists(List<OrderByItem> list1,
                                                                             List<OrderByItem> list2) {
        if (list1 == null || list2 == null) {
            throw new IllegalArgumentException("Lists must not be null");
        }
        // Check ASC
        if (list1.equals(list2)) {
            return Pair.of(OrderRelation.ASC, ImmutableList.copyOf(list2));
        }

        // Check PREFIX_ASC
        Pair<OrderRelation, List<OrderByItem>> result;
        if ((result = isPrefix(list1, list2, OrderRelation.PREFIX_ASC))
            .getKey() == OrderRelation.PREFIX_ASC) {
            return result;
        }

        // Check DESC
        List<OrderByItem> reversedList2 = new ArrayList<>();
        for (OrderByItem orderByItem : list2) {
            // reverse asc flag.
            reversedList2.add(new OrderByItem(orderByItem.columnName, !orderByItem.asc));
        }

        if (list1.equals(reversedList2)) {
            return Pair.of(OrderRelation.DESC, ImmutableList.copyOf(reversedList2));
        }

        // Check PREFIX_DESC
        if ((result = isPrefix(list1, reversedList2, OrderRelation.PREFIX_DESC))
            .getKey() == OrderRelation.PREFIX_DESC) {
            return result;
        }
        // No relation
        return Pair.of(OrderRelation.NONE, ImmutableList.of());
    }

    // Helper method: Check if prefix is a prefix of fullList
    private static Pair<OrderRelation, List<OrderByItem>> isPrefix(
        List<OrderByItem> prefix, List<OrderByItem> fullList, OrderRelation expected) {

        int prefixIndex = 0;
        for (; prefixIndex < Math.min(prefix.size(), fullList.size()); prefixIndex++) {
            if (!prefix.get(prefixIndex).equals(fullList.get(prefixIndex))) {
                break;
            }
        }

        if (prefixIndex == 0) {
            return Pair.of(OrderRelation.NONE, ImmutableList.of());
        }

        List<OrderByItem> list = new ArrayList<>();
        for (int i = 0; i < prefixIndex; i++) {
            OrderByItem orderByItem = fullList.get(i);
            list.add(new OrderByItem(orderByItem.columnName, orderByItem.asc));
        }

        return Pair.of(expected, list);
    }
}
