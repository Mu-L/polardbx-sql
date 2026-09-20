/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.DblePartitionAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByFileMap;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.RuleAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.BuildParams;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.DbleAlgorithmBuilder;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NewPartEnumUdfImpl extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public NewPartEnumUdfImpl() {
    }

    public NewPartEnumUdfImpl(int type, int defaultNode, Map<String,Integer> mappings) {
        List<Pair<String, Integer>> partMappings = new ArrayList<>();
        for (Map.Entry<String, Integer> mapItem : mappings.entrySet()) {
            partMappings.add(new Pair<>(mapItem.getKey(), mapItem.getValue()));
        }
        init(type, defaultNode, partMappings);
    }

    @Override
    public Object compute(Object[] args) {
        return  routeFunc.compute(args);
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
        /**
         * Map<Object, Integer> app2Partition key's type:default 0 means Integer,other means String
         */
        private int type = 0;

        /**
         * DEFAULT_NODE key
         */
        private static final String DEFAULT_NODE = "DEFAULT_NODE";

        /**
         * defaultNode:-1 means no default node ,other means the default node index
         * <p>
         * use defaultNode,the unexpected value will router to the default value.
         * Otherwise will report error like this:can't find shardingnode for sharding column:column_name val:ffffffff
         */
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
            propertiesMap.put("type", String.valueOf(type));
        }

        public void setDefaultNode(int defaultNode) {
            if (defaultNode >= 0 || defaultNode == -1) {
                this.defaultNode = defaultNode;
            }
            propertiesMap.put("defaultNode", String.valueOf(defaultNode));
        }

        @Override
        public Integer calculate(String columnValue) {
            try {
                if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
                    return app2Partition.get(DEFAULT_NODE);
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
            app2Partition = new HashMap<>();
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
            if (o == null || getClass() != o.getClass()) {
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
                Integer otherValue = other.app2Partition.get(entry.getKey());
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


}
