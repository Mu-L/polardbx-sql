/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.type.ConstraintType;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ConstraintUtils {
    public static String getCheckConstraintName(Set<String> addConstraints, String schemaName, String tableName) {
        // create check constraints symbol
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        Set<String> symbols = tableMeta.getConstraintsByType(ConstraintType.CHECK.name());
        String baseName = tableName.toLowerCase() + "_chk_";
        int prob = 1;
        while (symbols.contains(baseName + prob) || addConstraints.contains(baseName + prob)) {
            ++prob;
        }
        addConstraints.add(baseName + prob);

        return baseName + prob;
    }

    public static ConstraintType getConstraintType(String schemaName, String tableName, String constraintName) {
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        Map<String, String> constraint2typeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Set<String>> entry : tableMeta.getConstraints().entrySet()) {
            for (String constraint : entry.getValue()) {
                constraint2typeMap.put(constraint, entry.getKey());
            }
        }
        return ConstraintType.valueOf(constraint2typeMap.get(constraintName));
    }
}
