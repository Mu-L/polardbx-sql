-- delimiter |
drop java function if exists newpartenumt0d0i|
CREATE JAVA FUNCTION newpartenumt0d0i
  no state
  RETURN_TYPE bigint
  INPUT_TYPES bigint
CODE

  public class Newpartenumt0d0i extends UserDefinedJavaFunction {
         private DblePartitionAlgorithm routeFunc;
         public Newpartenumt0d0i() {

             int type = 0;
             int defaultNode = 0;

             List<Pair<String, Integer>> partMappings = new ArrayList<>();
             partMappings.add(new Pair<>("10000", 0));
             partMappings.add(new Pair<>("10010", 1));

             init(type, defaultNode, partMappings);
         }

        @Override
        public Object compute(Object[] args) {
            return routeFunc.compute(args);
        }


        protected void init(int type, int defaultNode, List<Pair<String, Integer>> mappings) {
            NewPartitionByFileMap newPartitionByFileMap = new NewPartitionByFileMap();
            newPartitionByFileMap.setType(type);
            newPartitionByFileMap.setDefaultNode(defaultNode);
            newPartitionByFileMap.setPartitionMappings(mappings);
            newPartitionByFileMap.init();
            routeFunc = newPartitionByFileMap;
        }

        public class NewPartitionByFileMap extends DblePartitionAlgorithm implements RuleAlgorithm {
            private static final long serialVersionUID = 1884866019947627284L;
            private String mapFile;
            private Map<Object, Integer> app2Partition;
            private int partitionNum = 0;
            private int type = 0;
            private final String DEFAULT_NODE = "DEFAULT_NODE";
            private int defaultNode = -1;
            private int hashCode = 1;

            public NewPartitionByFileMap() {
            }

            @Override
            public void init() {
                initializeByPartitionMappings();
                initHashCode();
            }

            @Override
            public void selfCheck() {
            }

            public void setMapFile(String mapFile) {
                this.mapFile = mapFile;
            }

            public String getMapFile() {
                return mapFile;
            }

            public void setType(int type) {
                this.type = type;
            }

            public void setDefaultNode(int defaultNode) {
                if (defaultNode >= 0 || defaultNode == -1) {
                    this.defaultNode = defaultNode;
                }
            }

            @Override
            public Integer calculate(String columnValue) {
                try {
                    if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
                        return (Integer) app2Partition.get(DEFAULT_NODE);
                    }

                    Object value = columnValue;
                    if (type == 0) {
                        value = Integer.valueOf(columnValue);
                    }
                    Integer rst;
                    Integer pid = (Integer) app2Partition.get(value);
                    if (pid != null) {
                        rst = pid;
                    } else {
                        rst =  (Integer) app2Partition.get(DEFAULT_NODE);
                    }
                    return rst;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("columnValue:" + columnValue + " Please check if the format satisfied.", e);
                }
            }

            @Override
            public Integer[] calculateRange(String beginValue, String endValue) {
                return new Integer[0];
            }

            @Override
            public int getPartitionNum() {
                return partitionNum;
            }

            private void initializeByPartitionMappings() {
                app2Partition = new HashMap<Object, Integer>();
                try {

                    if (this.partitionMappings != null) {
                        for (int i = 0; i <  this.partitionMappings.size(); i++) {
                            Pair<String, Integer> mapItem = (Pair<String, Integer>) this.partitionMappings.get(i);
                            String key = (String) mapItem.getKey();
                            Integer pid = (Integer) mapItem.getValue();
                            if (type == 0) {
                                app2Partition.put(Integer.parseInt(key), pid);
                            } else {
                                app2Partition.put(key, pid);
                            }
                        }
                    }

                    //set default node
                    if (defaultNode >= 0) {
                        app2Partition.put(DEFAULT_NODE, defaultNode);
                    }
                    Set<Integer> set = new HashSet<>(app2Partition.values());
                    partitionNum = set.size();
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    } else {
                        throw new RuntimeException(e);
                    }

                }
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null) {
                    return false;
                }
                NewPartitionByFileMap other = (NewPartitionByFileMap) o;
                if (other.defaultNode != defaultNode) {
                    return false;
                }
                if (other.type != type) {
                    return false;
                }
                if (other.app2Partition.size() != app2Partition.size()) {
                    return false;
                }
                for (Map.Entry<Object, Integer> entry : app2Partition.entrySet()) {
                    Integer otherValue = (Integer) other.app2Partition.get(entry.getKey());
                    if (!entry.getValue().equals(otherValue)) {
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
                if (type != 0) {
                    hashCode *= type;
                }
                hashCode *= app2Partition.size();
            }

        }

};
END_CODE|
-- delimiter ;
