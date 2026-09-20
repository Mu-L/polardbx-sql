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
package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

public class VectorCodecTest {

    @Test
    public void testFloat32LittleEndianRoundTrip() {
        byte[] binary = VectorCodec.encodeBinary(new float[] {1.0F, -2.5F});
        Assert.assertArrayEquals(new byte[] {0, 0, -128, 63, 0, 0, 32, -64}, binary);
        Assert.assertArrayEquals(new float[] {1.0F, -2.5F}, VectorCodec.decodeBinary(binary), 0.0F);
    }

    @Test
    public void testTextSyntaxAndFormatting() {
        float[] vector = VectorCodec.parseText(" [ +1.25e1, -2.5E-1, -0 ] ");
        Assert.assertArrayEquals(new float[] {12.5F, -0.25F, -0.0F}, vector, 0.0F);
        Assert.assertEquals("[12.5,-0.25,-0]", VectorCodec.formatText(vector));
        Assert.assertArrayEquals(new float[0], VectorCodec.parseText("[]"), 0.0F);
    }

    @Test
    public void testRejectsMalformedTextAndNonFiniteValues() {
        assertVectorError("[1,]");
        assertVectorError("[1,,2]");
        assertVectorError("[value]");
        assertVectorError("[NaN]");
        assertVectorError("[Infinity]");
        assertVectorError("1,2");
    }

    @Test
    public void testStrictJsonArrayAndNull() {
        Assert.assertArrayEquals(new float[] {1.0F, -0.25F, 300.0F},
            VectorCodec.parseJson("[1,-0.25,3e2]"), 0.0F);
        Assert.assertNull(VectorCodec.parseJson("null"));
        assertJsonError("{}");
        assertJsonError("1");
        assertJsonError("[[1],2]");
        assertJsonError("[1,\"2\"]");
        assertJsonError("[1,null]");
        assertJsonError("[1e1000]");
        assertJsonError("invalid");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRejectsInvalidBinaryLength() {
        VectorCodec.decodeBinary(new byte[3]);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testVectorTypeChecksDeclaredDimension() {
        new VectorType(3).convertFrom("[1,2]");
    }

    @Test
    public void testVectorTypeMetadataFollowsParameterizedTypeContract() {
        VectorType vectorType = new VectorType(3);
        Assert.assertEquals("VECTOR", vectorType.getStringSqlType());
        Assert.assertEquals(3, vectorType.getPrecision());
        Assert.assertEquals(12, vectorType.length());
        Assert.assertTrue(vectorType.equalDeeply(new VectorType(3)));
        Assert.assertFalse(vectorType.equalDeeply(new VectorType(2)));
    }

    @Test
    public void testDataTypeFactoryNormalizesUnspecifiedVectorDimension() {
        TddlTypeFactoryImpl typeFactory =
            new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        RelDataType relDataType = typeFactory.createSqlType(SqlTypeName.VECTOR);
        VectorType vectorType = (VectorType) DataTypeFactory.INSTANCE.create(relDataType);
        Assert.assertEquals(0, vectorType.getDimension());
        Assert.assertEquals("VECTOR", vectorType.getStringSqlType());
    }

    private static void assertVectorError(String text) {
        try {
            VectorCodec.parseText(text);
            Assert.fail("Expected VECTOR error for " + text);
        } catch (TddlRuntimeException expected) {
            Assert.assertFalse(expected.getMessage().contains("NumberFormatException"));
            Assert.assertFalse(expected.getMessage().contains("IllegalArgumentException"));
        }
    }

    private static void assertJsonError(String json) {
        try {
            VectorCodec.parseJson(json);
            Assert.fail("Expected VECTOR JSON error for " + json);
        } catch (TddlRuntimeException expected) {
            Assert.assertFalse(expected.getMessage().contains("JSONException"));
            Assert.assertFalse(expected.getMessage().contains("NumberFormatException"));
        }
    }
}
