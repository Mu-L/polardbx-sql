/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.DblePartitionAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.LongRange;

import com.alibaba.polardbx.optimizer.core.function.calc.dble.RuleAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.Pair;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

public class NewPatternRngUdfImpl extends UserDefinedJavaFunction {
    private DblePartitionAlgorithm routeFunc;

    public NewPatternRngUdfImpl() {
    }
    public NewPatternRngUdfImpl(int defaultNode, int partitionValue, List<Pair<String, Integer>> partitionMappings) {
        init(defaultNode, partitionValue, partitionMappings);
    }

    public static class NewPartitionByPattern extends DblePartitionAlgorithm implements RuleAlgorithm {
        private static final int PARTITION_LENGTH = 1024;
        private int patternValue = PARTITION_LENGTH; // mod value
        private String mapFile = null;
        private String ruleFile = null;
        private LongRange[] longRanges;
        private Integer[] allNode;
        private int defaultNode = -1; // default node for unexpected value
        private static final Pattern PATTERN = Pattern.compile("[0-9]*");
        private int hashCode = 1;

        public NewPartitionByPattern() {
        }

        @Override
        public void init() {
            initializeByPartitionMappings();
            initHashCode();
        }

        @Override
        public void selfCheck() {
        }


        public void setPatternValue(int patternValue) {
            this.patternValue = patternValue;
            propertiesMap.put("patternValue", String.valueOf(patternValue));
        }

        public void setDefaultNode(int defaultNode) {
            if (defaultNode >= 0 || defaultNode == -1) {
                this.defaultNode = defaultNode;
            }
            propertiesMap.put("defaultNode", String.valueOf(defaultNode));
        }

        private Integer findNode(long hash) {
            for (LongRange longRang : this.longRanges) {
                if (hash <= longRang.getValueEnd() && hash >= longRang.getValueStart()) {
                    return longRang.getNodeIndex();
                }
            }
            return null;
        }

        @Override
        public Integer calculate(String columnValue) {
            if (columnValue == null || columnValue.equalsIgnoreCase("NULL") || !isNumeric(columnValue)) {
                return defaultNode < 0 ? null : defaultNode;
            }

            long value = Long.parseLong(columnValue);
            long hash = value % patternValue;
            return findNode(hash);
        }

        /* x2 - x1 < m
         *     n1 < n2 ---> n1 - n2          type1
         *     n1 > n2 ---> 0 - n2 && n1 - L      type2
         * x2 - x1 >= m
         *     L                 type3
         */
        private void calcAux(HashSet<Integer> ids, long begin, long end) {
            for (LongRange longRang : this.longRanges) {
                if (longRang.getValueEnd() < begin) {
                    continue;
                }
                if (longRang.getValueStart() > end) {
                    break;
                }
                ids.add(longRang.getNodeIndex());
            }
        }

        private Integer[] calcType1(long begin, long end) {
            HashSet<Integer> ids = new HashSet<>();

            calcAux(ids, begin, end);

            return ids.toArray(new Integer[ids.size()]);
        }

        private Integer[] calcType2(long begin, long end) {
            HashSet<Integer> ids = new HashSet<>();

            calcAux(ids, begin, patternValue);
            calcAux(ids, 0, end);

            return ids.toArray(new Integer[ids.size()]);
        }

        private Integer[] calcType3() {
            return allNode;
        }

        /* NODE:
         * if the order of range and the order of nodeid don't match, we give all nodeid.
         * so when writing configure, be cautious
         */
        public Integer[] calculateRange(String beginValue, String endValue) {
            if (!isNumeric(beginValue) || !isNumeric(endValue)) {
                return calcType3();
            }
            long bv = Long.parseLong(beginValue);
            long ev = Long.parseLong(endValue);
            long hbv = bv % patternValue;
            long hev = ev % patternValue;

            if (findNode(hbv) == null || findNode(hev) == null) {
                return calcType3();
            }

            if (ev >= bv) {
                if (ev - bv >= patternValue) {
                    return calcType3();
                }

                if (hbv < hev) {
                    return calcType1(hbv, hev);
                } else {
                    return calcType2(hbv, hev);
                }
            } else {
                return new Integer[0];
            }
        }

        @Override
        public int getPartitionNum() {
            return this.longRanges.length;
        }

        private static boolean isNumeric(String str) {
            return PATTERN.matcher(str).matches();
        }

        private void initializeAux(LinkedList<LongRange> ll, LongRange lr) {
            if (ll.size() == 0) {
                ll.add(lr);
            } else {
                LongRange tmp;
                for (int i = ll.size() - 1; i > -1; i--) {
                    tmp = ll.get(i);
                    if (tmp.getValueStart() < lr.getValueStart()) {
                        ll.add(i + 1, lr);
                        return;
                    }
                }
                ll.add(0, lr);
            }
        }

        private void initializeByPartitionMappings() {
            try {
                LinkedList<LongRange> longRangeList = new LinkedList<>();
                HashSet<Integer> ids = new HashSet<>();
                for (int i = 0; i < partitionMappings.size(); ++i) {
                    Pair<String, Integer> mapItem = partitionMappings.get(i);
                    String key = mapItem.getKey();
                    String[] pairs = key.split("-");
                    long longStart = Long.parseLong(pairs[0].trim());
                    long longEnd = Long.parseLong(pairs[1].trim());
                    int nodeId = mapItem.getValue();
                    ids.add(nodeId);
                    initializeAux(longRangeList, new LongRange(nodeId, longStart, longEnd));
                }
                allNode = ids.toArray(new Integer[ids.size()]);
                longRanges = longRangeList.toArray(new LongRange[longRangeList.size()]);
//            propertiesMap.put("mapFile", sb.toString());
            } catch (Exception e) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            } finally {
                try {
                } catch (Exception e2) {
                    //ignore error
                }
            }
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            NewPartitionByPattern other = (NewPartitionByPattern) o;

            if (other.defaultNode != defaultNode) {
                return false;
            }
            if (other.patternValue != patternValue) {
                return false;
            }
            for (int i = 0; i < longRanges.length; i++) {
                if (!other.longRanges[i].equals(longRanges[i])) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        private void initHashCode() {
            hashCode *= patternValue;
            if (defaultNode != 0) {
                hashCode *= defaultNode;
            }
            for (LongRange longRange : longRanges) {
                hashCode *= longRange.hashCode();
            }
        }
    }

    private void init(int defaultNode, int partitionValue, List<Pair<String, Integer>> partitionMappings) {
        NewPartitionByPattern newPatternRngUdf = new NewPartitionByPattern();
        newPatternRngUdf.setDefaultNode(defaultNode);
        newPatternRngUdf.setPatternValue(partitionValue);
        newPatternRngUdf.setPartitionMappings(partitionMappings);
        newPatternRngUdf.init();
        routeFunc = newPatternRngUdf;
    }


    @Override
    public Object compute(Object[] args) {
        return routeFunc.compute(args);
    }
}
