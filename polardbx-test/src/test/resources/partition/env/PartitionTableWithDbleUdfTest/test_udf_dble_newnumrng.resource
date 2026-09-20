-- delimiter |
drop java function if exists newnumrngd0r3i|
CREATE JAVA FUNCTION newnumrngd0r3i
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Newnumrngd0r3i extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Newnumrngd0r3i() {
             int defaultNode = 0;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("0-200M", 0));
             partMappings.add(new Pair<>("200M1-400M", 1));
             partMappings.add(new Pair<>("400M1-600M", 2));

             init(defaultNode, partMappings);
         }

         @Override
         public Object compute(Object[] args) {
             return routeFunc.compute(args);
         }

        private void init(int defaultNode, List<Pair<String, Integer>> partitionMappings) {
            NewAutoPartitionByLong newAutoPartitionByLong = new NewAutoPartitionByLong();
            newAutoPartitionByLong.setDefaultNode(defaultNode);
            newAutoPartitionByLong.setPartitionMappings(partitionMappings);
            newAutoPartitionByLong.init();
            routeFunc = newAutoPartitionByLong;
        }

        public class NewAutoPartitionByLong extends DblePartitionAlgorithm implements RuleAlgorithm {
            private static final long serialVersionUID = 5752372920655270639L;
            private LongRange[] longRanges;
            private int defaultNode = -1;
            private int hashCode = 1;

            @Override
            public void init() {
                initializeByPartitionConfigs();
                initHashCode();
            }

            @Override
            public void selfCheck() {
            }


            @Override
            public Integer calculate(String columnValue) {
                try {

                    if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
                        if (defaultNode >= 0) {
                            return defaultNode;
                        }
                        return null;
                    }

                    long value = Long.parseLong(columnValue);
                    for (LongRange longRang : this.longRanges) {
                        if (value <= longRang.getValueEnd() && value >= longRang.getValueStart()) {
                            return longRang.getNodeIndex();
                        }
                    }
                    // use default node for other value
                    if (defaultNode >= 0) {
                        return defaultNode;
                    }
                    return null;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
                }
            }

            /**
             * @param columnValue
             * @return
             */
            public boolean isUseDefaultNode(String columnValue) {
                try {
                    long value = Long.parseLong(columnValue);
                    for (LongRange longRang : this.longRanges) {
                        if (value <= longRang.getValueEnd() && value >= longRang.getValueStart()) {
                            return false;
                        }
                    }
                    if (defaultNode >= 0) {
                        return true;
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
                }
                return false;
            }


            @Override
            public Integer[] calculateRange(String beginValue, String endValue) {
                Integer begin = 0, end = 0;
                if (isUseDefaultNode(beginValue) || isUseDefaultNode(endValue)) {
                    begin = 0;
                    end = longRanges.length - 1;
                } else {
                    begin = calculate(beginValue);
                    end = calculate(endValue);
                }


                if (begin == null || end == null) {
                    return new Integer[0];
                }
                if (end >= begin) {
                    int len = end - begin + 1;
                    Integer[] re = new Integer[len];

                    for (int i = 0; i < len; i++) {
                        re[i] = begin + i;
                    }
                    return re;
                } else {
                    return new Integer[0];
                }
            }

            @Override
            public int getPartitionNum() {
                return longRanges.length;
            }

            private void initializeByPartitionConfigs() {
                StringBuilder sb = new StringBuilder("{");
                LinkedList<LongRange> longRangeList = new LinkedList<>();
                try {
                    if (this.partitionMappings != null) {
                        for (int i = 0; i <  this.partitionMappings.size(); i++) {
                            Pair<String, Integer> mapItem = (Pair<String, Integer>) this.partitionMappings.get(i);
                            String key = (String) mapItem.getKey();
                            Integer value = (Integer) mapItem.getValue();
                            String[] pairs = key.split("-");
                            long longStart = NumberParseUtil.parseLong(pairs[0].trim());
                            long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
                            int nodeId = value;
                            longRangeList.add(new LongRange(nodeId, longStart, longEnd));
                        }
                    }
                    longRanges = (LongRange[]) longRangeList.toArray(new LongRange[longRangeList.size()]);

                    propertiesMap.put("mapFiles", partitionMappings.toString());
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }

                } finally {
                }
            }


            public void setDefaultNode(int defaultNode) {
                if (defaultNode >= 0 || defaultNode == -1) {
                    this.defaultNode = defaultNode;
                }
                propertiesMap.put("defaultNode", String.valueOf(defaultNode));
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null) {
                    return false;
                }
                NewAutoPartitionByLong other = (NewAutoPartitionByLong) o;
                if (other.defaultNode != defaultNode) {
                    return false;
                }
                if (other.longRanges.length != longRanges.length) {
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
                if (defaultNode != 0) {
                    hashCode *= defaultNode;
                }
                for (LongRange longRange : longRanges) {
                    hashCode *= longRange.hashCode();
                }
            }
        }

};
END_CODE|
-- delimiter ;
