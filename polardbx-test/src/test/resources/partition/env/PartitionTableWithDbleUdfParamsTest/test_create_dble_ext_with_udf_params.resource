delimiter $
drop java function if exists mynewpartlong $
CREATE JAVA FUNCTION mynewpartlong
  RETURN_TYPE bigint
  INPUT_TYPES varchar(255)
CODE

  public class Mynewpartlong extends UserDefinedParameterizedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Mynewpartlong() {
         }

        @Override
        protected void initFunction(String initParams) {
            Map<String, Object> algorithmParamsMappings = (Map<String, Object>) DbleAlgorithmInitParams.buildMappingsFromJsonParams(initParams);
            String partitionCount = (String) algorithmParamsMappings.get("partitionCount");
            String partitionLength = (String) algorithmParamsMappings.get("partitionLength");
            init(partitionCount, partitionLength);
            initParamsInfo = (Map<String, Object>) algorithmParamsMappings;
        }

        private void init(String count, String partLen) {
            PartNewLong newPartLong = new PartNewLong();
            newPartLong.setPartitionCount(count);
            newPartLong.setPartitionLength(partLen);
            newPartLong.init();
            routeFunc = newPartLong;
        }

        public static class PartNewLong extends DblePartitionAlgorithm implements RuleAlgorithm {
            private static final long serialVersionUID = -471239908303025898L;
            protected int[] count;
            protected int[] length;
            private PartitionUtil partitionUtil;
            private int hashCode = 1;

            public PartNewLong() {
            }

            public void setPartitionCount(String partitionCount) {
                this.count = toIntArray(partitionCount);
                propertiesMap.put("partitionCount", partitionCount);
            }

            public void setPartitionLength(String partitionLength) {
                this.length = toIntArray(partitionLength);
                propertiesMap.put("partitionLength", partitionLength);
            }

            @Override
            public void init() {
                partitionUtil = new PartitionUtil(count, length);
                initHashCode();
            }

            @Override
            public void selfCheck() {
            }

            @Override
            public Integer calculate(String columnValue) {
                try {
                    if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
                        return 0;
                    }
                    long key = Long.parseLong(columnValue);
                    return calculate(key);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
                }
            }

            private Integer calculate(long key) {
                return partitionUtil.partition(key);
            }

            @Override
            public Integer[] calculateRange(String beginValue, String endValue) {
                long begin;
                long end;
                try {
                    begin = Long.parseLong(beginValue);
                    end = Long.parseLong(endValue);
                } catch (NumberFormatException e) {
                    return new Integer[0];
                }
                int partitionLength = partitionUtil.getPartitionLength();
                if (end - begin >= partitionLength || begin > end) { //TODO: optimize begin > end
                    return new Integer[0];
                }
                Integer beginNode = calculate(begin);
                Integer endNode = calculate(end);

                if (endNode > beginNode || (endNode.equals(beginNode) && partitionUtil.isSingleNode(begin, end))) {
                    int len = endNode - beginNode + 1;
                    Integer[] re = new Integer[len];

                    for (int i = 0; i < len; i++) {
                        re[i] = beginNode + i;
                    }
                    return re;
                } else {
                    int split = partitionUtil.getSegmentLength() - beginNode;
                    int len = split + endNode + 1;
                    if (endNode.equals(beginNode)) {
                        //remove duplicate
                        len--;
                    }
                    Integer[] re = new Integer[len];
                    for (int i = 0; i < split; i++) {
                        re[i] = beginNode + i;
                    }
                    for (int i = split; i < len; i++) {
                        re[i] = i - split;
                    }
                    return re;
                }
            }

            @Override
            public int getPartitionNum() {
                return partitionUtil.getSegmentLength();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                PartNewLong other = (PartNewLong) o;
                if (other.count.length != this.count.length || other.length.length != this.length.length) {
                    return false;
                }
                for (int i = 0; i < count.length; i++) {
                    if (this.count[i] != other.count[i]) {
                        return false;
                    }
                }
                for (int i = 0; i < length.length; i++) {
                    if (this.length[i] != other.length[i]) {
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
                for (int aCount : count) {
                    hashCode *= aCount;
                }
                for (int aLength : length) {
                    hashCode *= aLength;
                }
            }

            private int[] toIntArray(String string) {
                String[] strNumbers = SplitUtil.split(string, ',', true);
                int[] numbers = new int[strNumbers.length];
                for (int i = 0; i < strNumbers.length; ++i) {
                    numbers[i] = Integer.parseInt(strNumbers[i]);
                }
                return numbers;
            }
        }

        @Override
        public Object compute(Object[] args) {
            return  routeFunc.compute(args);
        }
};
END_CODE $
delimiter ;
