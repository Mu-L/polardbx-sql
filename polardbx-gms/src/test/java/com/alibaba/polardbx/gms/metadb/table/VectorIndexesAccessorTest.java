/*
 * Copyright 2019 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VectorIndexesAccessorTest {

    @Test
    public void testSpecifiedIndexesUseStructuredViewColumnsAndParameters() {
        CapturingAccessor accessor = new CapturingAccessor();

        accessor.query("phy_db", "phy_table", Arrays.asList("vec_a", "vec_b"), mock(DataSource.class));

        Assert.assertTrue(accessor.sql, accessor.sql.contains("from information_schema.vector_indexes"));
        Assert.assertTrue(accessor.sql, accessor.sql.contains("`metric_type`, `dimension`, `m`, `ef_construction`"));
        Assert.assertTrue(accessor.sql, accessor.sql.endsWith("and `index_name` in (?,?)"));
        Assert.assertEquals("phy_db", accessor.params.get(1).getValue());
        Assert.assertEquals("phy_table", accessor.params.get(2).getValue());
        Assert.assertEquals("vec_a", accessor.params.get(3).getValue());
        Assert.assertEquals("vec_b", accessor.params.get(4).getValue());
        Assert.assertEquals(VectorIndexesInfoSchemaRecord.class, accessor.recordClass);
    }

    @Test
    public void testRecordMapsStructuredViewFieldsIncludingNullDimension() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("table_schema")).thenReturn("phy_db");
        when(resultSet.getString("table_name")).thenReturn("phy_table");
        when(resultSet.getString("index_name")).thenReturn("vec_a");
        when(resultSet.getString("column_name")).thenReturn("embedding");
        when(resultSet.getString("algorithm")).thenReturn("HNSW");
        when(resultSet.getString("metric_type")).thenReturn("COSINE");
        when(resultSet.getLong("dimension")).thenReturn(0L);
        when(resultSet.wasNull()).thenReturn(true);
        when(resultSet.getString("m")).thenReturn("16");
        when(resultSet.getString("ef_construction")).thenReturn("100");

        VectorIndexesInfoSchemaRecord record = new VectorIndexesInfoSchemaRecord().fill(resultSet);

        Assert.assertEquals("phy_db", record.tableSchema);
        Assert.assertEquals("phy_table", record.tableName);
        Assert.assertEquals("vec_a", record.indexName);
        Assert.assertEquals("embedding", record.columnName);
        Assert.assertEquals("HNSW", record.algorithm);
        Assert.assertEquals("COSINE", record.metricType);
        Assert.assertNull(record.dimension);
        Assert.assertEquals("16", record.m);
        Assert.assertEquals("100", record.efConstruction);
    }

    private static class CapturingAccessor extends VectorIndexesAccessor {
        private String sql;
        private Map<Integer, ParameterContext> params;
        private Class recordClass;

        @Override
        protected <T extends SystemTableRecord> List<T> query(String selectSql, String systemTable, Class clazz,
                                                              Map<Integer, ParameterContext> parameters,
                                                              DataSource dataSource) {
            this.sql = selectSql;
            this.params = parameters;
            this.recordClass = clazz;
            return Collections.emptyList();
        }
    }
}
