/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.common.utils.Column;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.Table;
import com.alibaba.polardbx.common.utils.UnionFind;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.rel.metadata.RelColumnOrigin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author dylan
 */
public class IndexableColumnSet {

    // {schema -> table -> column}
    Map<String, Map<String, Set<String>>> m;

    Map<String, Map<String, Set<String>>> partColumns;
    UnionFind<Table> joinTbs;

    UnionFind<Column> joinCols;

    public IndexableColumnSet() {
        this.m = new HashMap<>();
        this.partColumns = new HashMap<>();
        this.joinTbs = new UnionFind<>();
        this.joinCols = new UnionFind<>();

    }

    public void addIndexableColumn(RelColumnOrigin columnOrigin) {
        addIndexableColumn(columnOrigin, false);
    }

    public void addIndexableColumn(RelColumnOrigin columnOrigin, boolean partColumn) {
        if (columnOrigin != null) {
            TableMeta tableMeta = CBOUtil.getTableMeta(columnOrigin.getOriginTable());
            String schemaName = tableMeta.getSchemaName();
            String tableName = tableMeta.getTableName();
            String columnName = columnOrigin.getColumnName();
            ColumnMeta columnMeta = tableMeta.getAllColumns().get(columnOrigin.getOriginColumnOrdinal());
            if (StatisticUtils.isBinaryOrJsonColumn(columnMeta)) {
                return;
            }
            if (DataTypeUtil.isStringType(columnMeta.getDataType())) {
                if (columnMeta.getField().getPrecision() > 1000) {
                    return;
                }
            }

            if (partColumn) {
                this.addPartColumn(schemaName, tableName, columnName);
            } else {
                this.addIndexableColumn(schemaName, tableName, columnName);
            }
        }
    }

    public void addJoinColumns(RelColumnOrigin leftColumnOrigin, RelColumnOrigin rightColumnOrigin) {
        if (leftColumnOrigin != null && rightColumnOrigin != null) {
            TableMeta leftTableMeta = CBOUtil.getTableMeta(leftColumnOrigin.getOriginTable());
            String leftSchemaName = leftTableMeta.getSchemaName();
            String leftTableName = leftTableMeta.getTableName();
            String leftColumnName = leftColumnOrigin.getColumnName();
            ColumnMeta leftColumnMeta = leftTableMeta.getAllColumns().get(leftColumnOrigin.getOriginColumnOrdinal());
            if (StatisticUtils.isBinaryOrJsonColumn(leftColumnMeta)) {
                return;
            }
            if (DataTypeUtil.isStringType(leftColumnMeta.getDataType())) {
                if (leftColumnMeta.getField().getPrecision() > 1000) {
                    return;
                }
            }

            TableMeta rightTableMeta = CBOUtil.getTableMeta(rightColumnOrigin.getOriginTable());
            String rightSchemaName = rightTableMeta.getSchemaName();
            String rightTableName = rightTableMeta.getTableName();
            String rightColumnName = rightColumnOrigin.getColumnName();
            ColumnMeta rightColumnMeta = rightTableMeta.getAllColumns().get(rightColumnOrigin.getOriginColumnOrdinal());
            if (StatisticUtils.isBinaryOrJsonColumn(rightColumnMeta)) {
                return;
            }
            if (DataTypeUtil.isStringType(rightColumnMeta.getDataType())) {
                if (rightColumnMeta.getField().getPrecision() > 1000) {
                    return;
                }
            }

            Table leftTb = Table.of(leftSchemaName, leftTableName);
            Table rightTb = Table.of(rightSchemaName, rightTableName);
            joinTbs.add(leftTb);
            joinTbs.add(rightTb);
            joinTbs.union(leftTb, rightTb);

            Column leftCol = Column.of(leftSchemaName, leftTableName, leftColumnName);
            Column rightCol = Column.of(rightSchemaName, rightTableName, rightColumnName);
            joinCols.add(leftCol);
            joinCols.add(rightCol);
            joinCols.union(leftCol, rightCol);
        }
    }

    private void addIndexableColumn(String schemaName, String tableName, String columnName) {
        Map<String, Set<String>> t = m.get(schemaName);
        if (t == null) {
            t = new HashMap<>();
            m.put(schemaName, t);
        }
        Set<String> c = t.get(tableName);
        if (c == null) {
            c = new HashSet<>();
            t.put(tableName, c);
        }
        c.add(columnName);
    }

    private void addPartColumn(String schemaName, String tableName, String columnName) {
        Map<String, Set<String>> t = partColumns.get(schemaName);
        if (t == null) {
            t = new HashMap<>();
            partColumns.put(schemaName, t);
        }
        Set<String> c = t.get(tableName);
        if (c == null) {
            c = new HashSet<>();
            t.put(tableName, c);
        }
        c.add(columnName);
    }

}
