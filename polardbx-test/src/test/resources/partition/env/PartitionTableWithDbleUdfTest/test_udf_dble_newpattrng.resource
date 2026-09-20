-- delimiter |
drop java function if exists newpattrngd2p256i |
CREATE JAVA FUNCTION newpattrngd2p256i
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Newpattrngd2p256i extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Newpattrngd2p256i() {
             int defaultNode = 2;
             int partitionValue = 256;

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

             init(defaultNode, partitionValue, partMappings);
         }

        @Override
        public Object compute(Object[] args) {
            return routeFunc.compute(args);
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
                return (Integer[]) ids.toArray(new Integer[ids.size()]);

            }

            private Integer[] calcType2(long begin, long end) {
                HashSet<Integer> ids = new HashSet<>();

                calcAux(ids, begin, patternValue);
                calcAux(ids, 0, end);

                return (Integer[]) ids.toArray(new Integer[ids.size()]);
            }

            private Integer[] calcType3() {
                return allNode;
            }

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
                        tmp = (LongRange) ll.get(i);
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
                        Pair<String, Integer> mapItem = (Pair<String, Integer>) partitionMappings.get(i);
                        String key = (String) mapItem.getKey();
                        String[] pairs = (String[]) key.split("-");
                        long longStart = (Long) Long.parseLong(pairs[0].trim());
                        long longEnd = (Long) Long.parseLong(pairs[1].trim());
                        Integer nodeId = (Integer) mapItem.getValue();
                        ids.add(nodeId);
                        initializeAux(longRangeList, new LongRange(nodeId, longStart, longEnd));
                    }
                    allNode = (Integer[]) ids.toArray(new Integer[ids.size()]);
                    longRanges = (LongRange[]) longRangeList.toArray(new LongRange[longRangeList.size()]);

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
};
END_CODE|
-- delimiter ;
