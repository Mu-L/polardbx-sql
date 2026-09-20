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
package com.alibaba.polardbx.repo.mysql;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MysqlTableMetaManagerVectorTest {

    @Test
    public void testDirectShowIndexConversionKeepsVectorMetadataIsolated() throws Exception {
        ColumnMeta embedding = mock(ColumnMeta.class);
        when(embedding.getName()).thenReturn("embedding");
        Map<String, ColumnMeta> columns = Collections.singletonMap("embedding", embedding);

        Object vector = secondaryIndexMeta("vec_embedding", IndexType.VECTOR);
        invokeSetVectorMetadata(vector, "M=16, DISTANCE=COSINE, EF_CONSTRUCTION=100, DIM=128");
        IndexMeta vectorIndex = convertSecondaryIndexMeta(vector, columns);
        Assert.assertEquals(IndexType.VECTOR, vectorIndex.getIndexType());
        Assert.assertEquals(128, vectorIndex.getVectorIndexMeta().getDimension());

        Object ordinary = secondaryIndexMeta("idx_embedding", IndexType.BTREE);
        IndexMeta ordinaryIndex = convertSecondaryIndexMeta(ordinary, columns);
        Assert.assertEquals(IndexType.BTREE, ordinaryIndex.getIndexType());
        Assert.assertNull(ordinaryIndex.getVectorIndexMeta());
    }

    private static Object secondaryIndexMeta(String name, IndexType indexType) throws Exception {
        Class<?> type = Class.forName(MysqlTableMetaManager.class.getName() + "$SecondaryIndexMeta");
        Constructor<?> constructor = type.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object meta = constructor.newInstance();
        setField(type, meta, "name", name);
        setField(type, meta, "unique", false);
        setField(type, meta, "keys", Collections.singletonList("embedding"));
        setField(type, meta, "values", Collections.emptyList());
        setField(type, meta, "indexType", indexType);
        return meta;
    }

    private static void invokeSetVectorMetadata(Object meta, String comment) throws Exception {
        Method method = MysqlTableMetaManager.class.getDeclaredMethod("setVectorIndexMetadata", meta.getClass(),
            String.class);
        method.setAccessible(true);
        method.invoke(null, meta, comment);
    }

    private static IndexMeta convertSecondaryIndexMeta(Object meta, Map<String, ColumnMeta> columns)
        throws Exception {
        Method method = MysqlTableMetaManager.class.getDeclaredMethod("convertFromSecondaryIndexMeta",
            meta.getClass(), Map.class, String.class, boolean.class);
        method.setAccessible(true);
        return (IndexMeta) method.invoke(null, meta, columns, "t", true);
    }

    private static void setField(Class<?> type, Object target, String name, Object value) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
