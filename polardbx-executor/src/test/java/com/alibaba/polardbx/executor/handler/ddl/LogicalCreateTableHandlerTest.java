package com.alibaba.polardbx.executor.handler.ddl;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class LogicalCreateTableHandlerTest {

    @Test
    public void testBuildCtasColumnTypeSpec_EnumType() {
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType enumType = factory.createEnumSqlType(SqlTypeName.ENUM,
            Arrays.asList("user", "admin", "guest"));

        SqlIdentifier typeId = new SqlIdentifier("ENUM", SqlParserPos.ZERO);
        SqlDataTypeSpec result = LogicalCreateTableHandler.buildCtasColumnTypeSpec(
            typeId, enumType, -1, -1, null);

        assertNotNull(result);
        assertEquals("ENUM", result.getTypeName().getSimple());

        SqlNodeList collectionVals = result.getCollectionVals();
        assertNotNull(collectionVals);
        assertEquals(3, collectionVals.size());
        assertEquals("user", ((SqlLiteral) collectionVals.get(0)).toValue());
        assertEquals("admin", ((SqlLiteral) collectionVals.get(1)).toValue());
        assertEquals("guest", ((SqlLiteral) collectionVals.get(2)).toValue());

        assertNull(result.getCharSetName());
    }

    @Test
    public void testBuildCtasColumnTypeSpec_EnumTypeWithCharset() {
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType enumType = factory.createEnumSqlType(SqlTypeName.ENUM,
            Arrays.asList("active", "inactive"));

        SqlIdentifier typeId = new SqlIdentifier("ENUM", SqlParserPos.ZERO);
        SqlDataTypeSpec result = LogicalCreateTableHandler.buildCtasColumnTypeSpec(
            typeId, enumType, -1, -1, "utf8mb4");

        assertNotNull(result);
        assertEquals("ENUM", result.getTypeName().getSimple());

        SqlNodeList collectionVals = result.getCollectionVals();
        assertNotNull(collectionVals);
        assertEquals(2, collectionVals.size());
        assertEquals("active", ((SqlLiteral) collectionVals.get(0)).toValue());
        assertEquals("inactive", ((SqlLiteral) collectionVals.get(1)).toValue());

        assertEquals("utf8mb4", result.getCharSetName());
    }

    @Test
    public void testBuildCtasColumnTypeSpec_VarcharWithPrecision() {
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType varcharType = factory.createSqlType(SqlTypeName.VARCHAR, 255);

        SqlIdentifier typeId = new SqlIdentifier("VARCHAR", SqlParserPos.ZERO);
        SqlDataTypeSpec result = LogicalCreateTableHandler.buildCtasColumnTypeSpec(
            typeId, varcharType, 255, -1, null);

        assertNotNull(result);
        assertEquals("VARCHAR", result.getTypeName().getSimple());
        assertEquals(255, result.getPrecision());
        assertNull(result.getCharSetName());
    }

    @Test
    public void testBuildCtasColumnTypeSpec_VarcharNegativePrecision_FallsBackToText() {
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType longtextType = factory.createSqlType(SqlTypeName.VARCHAR, -1);

        SqlIdentifier typeId = new SqlIdentifier("VARCHAR", SqlParserPos.ZERO);
        SqlDataTypeSpec result = LogicalCreateTableHandler.buildCtasColumnTypeSpec(
            typeId, longtextType, -1, -1, null);

        assertNotNull(result);
        assertEquals("TEXT", result.getTypeName().getSimple());
    }

    @Test
    public void testBuildCtasColumnTypeSpec_BigInt() {
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType bigintType = factory.createSqlType(SqlTypeName.BIGINT);

        SqlIdentifier typeId = new SqlIdentifier("BIGINT", SqlParserPos.ZERO);
        SqlDataTypeSpec result = LogicalCreateTableHandler.buildCtasColumnTypeSpec(
            typeId, bigintType, -1, -1, null);

        assertNotNull(result);
        assertEquals("BIGINT", result.getTypeName().getSimple());
        assertEquals(0, result.getPrecision());
        assertEquals(0, result.getScale());
    }

    @Test
    public void testBuildCtasColumnTypeSpec_DecimalWithPrecisionAndScale() {
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType decimalType = factory.createSqlType(SqlTypeName.DECIMAL, 10, 2);

        SqlIdentifier typeId = new SqlIdentifier("DECIMAL", SqlParserPos.ZERO);
        SqlDataTypeSpec result = LogicalCreateTableHandler.buildCtasColumnTypeSpec(
            typeId, decimalType, 10, 2, null);

        assertNotNull(result);
        assertEquals("DECIMAL", result.getTypeName().getSimple());
        assertEquals(10, result.getPrecision());
        assertEquals(2, result.getScale());
    }

    @Test
    public void testBuildCtasColumnTypeSpec_VarcharWithCharset() {
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType varcharType = factory.createSqlType(SqlTypeName.VARCHAR, 100);

        SqlIdentifier typeId = new SqlIdentifier("VARCHAR", SqlParserPos.ZERO);
        SqlDataTypeSpec result = LogicalCreateTableHandler.buildCtasColumnTypeSpec(
            typeId, varcharType, 100, -1, "utf8mb4");

        assertNotNull(result);
        assertEquals("VARCHAR", result.getTypeName().getSimple());
        assertEquals(100, result.getPrecision());
        assertEquals("utf8mb4", result.getCharSetName());
    }

    @Test
    public void testBuildCtasColumnTypeSpec_CharWithPrecision() {
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType charType = factory.createSqlType(SqlTypeName.CHAR, 10);

        SqlIdentifier typeId = new SqlIdentifier("CHAR", SqlParserPos.ZERO);
        SqlDataTypeSpec result = LogicalCreateTableHandler.buildCtasColumnTypeSpec(
            typeId, charType, 10, -1, null);

        assertNotNull(result);
        assertEquals("CHAR", result.getTypeName().getSimple());
        assertEquals(10, result.getPrecision());
    }

    /**
     * AONE-84985809: CTAS 新增列时，java.nio.charset.Charset.name() 返回的原始 Java charset
     * 名称（如 "UTF-8"）被直接传给 buildCtasColumnTypeSpec 作为 MySQL charset，未经 CharsetName
     * 规范化，导致生成的 DDL 含 "CHARACTER SET UTF-8"，FastSQL 把 '-' 解析为运算符而报语法错误。
     */
    @Test
    public void testBuildCtasColumnTypeSpec_VarcharWithRawJavaCharsetName_MustBeNormalized() {
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType varcharType = factory.createSqlType(SqlTypeName.VARCHAR, 255);

        String rawJavaCharsetName = Charset.forName("UTF-8").name();
        assertEquals("UTF-8", rawJavaCharsetName);

        SqlIdentifier typeId = new SqlIdentifier("VARCHAR", SqlParserPos.ZERO);
        SqlDataTypeSpec result = LogicalCreateTableHandler.buildCtasColumnTypeSpec(
            typeId, varcharType, 255, -1, rawJavaCharsetName);

        assertNotNull(result);
        assertEquals("VARCHAR", result.getTypeName().getSimple());
        assertNotNull("charset must not be dropped", result.getCharSetName());
        assertEquals("false: legal MySQL charset name must not contain '-'",
            false, result.getCharSetName().contains("-"));
        assertEquals("utf8", result.getCharSetName());
    }

    @Test
    public void testBuildCtasColumnTypeSpec_EnumWithRawJavaCharsetName_MustBeNormalized() {
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType enumType = factory.createEnumSqlType(SqlTypeName.ENUM,
            Arrays.asList("active", "inactive"));

        String rawJavaCharsetName = Charset.forName("UTF-8").name();
        assertEquals("UTF-8", rawJavaCharsetName);

        SqlIdentifier typeId = new SqlIdentifier("ENUM", SqlParserPos.ZERO);
        SqlDataTypeSpec result = LogicalCreateTableHandler.buildCtasColumnTypeSpec(
            typeId, enumType, -1, -1, rawJavaCharsetName);

        assertNotNull(result);
        assertEquals("ENUM", result.getTypeName().getSimple());
        assertNotNull("charset must not be dropped", result.getCharSetName());
        assertEquals("false: legal MySQL charset name must not contain '-'",
            false, result.getCharSetName().contains("-"));
        assertEquals("utf8", result.getCharSetName());
    }
}
