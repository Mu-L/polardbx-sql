/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.optimizer.json;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJsonTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link SqlJsonTable}.
 */
@RunWith(MockitoJUnitRunner.class)
public class SqlJsonTableTest {

    @Mock
    private SqlWriter mockWriter;

    @Mock
    private SqlWriter.Frame mockFrame;

    @Mock
    private SqlValidator mockValidator;

    @Mock
    private SqlValidatorScope mockScope;

    @Mock
    private SqlIdentifier mockJsonExpr;

    @Mock
    private SqlDynamicParam mockPathExpr;

    @Mock
    private SqlIdentifier mockColumnName;

    @Mock
    private SqlDataTypeSpec mockDataType;

    @Mock
    private SqlDynamicParam mockColumnPath;

    private SqlJsonTable target;
    private SqlParserPos pos;

    @Before
    public void setUp() {
        pos = SqlParserPos.ZERO;

        // Setup mock behaviors
        doNothing().when(mockWriter).keyword(any());

    }

    /**
     * Test edge case with empty columns list to cover the empty columns branch.
     */
    @Test
    public void testUnparseWithEmptyColumns() {
        // Create SqlJsonTable with empty columns list
        target = new SqlJsonTable(mockJsonExpr, mockPathExpr, new ArrayList<>(), pos);

        // Test unparse with empty columns
        target.unparse(mockWriter, 0, 0);

        // Verify basic structure without COLUMNS section (SqlJsonTable.unparse behavior)
        verify(mockWriter, times(1)).keyword("JSON_TABLE");
        verify(mockWriter, times(1)).keyword("(");
        verify(mockJsonExpr, times(1)).unparse(mockWriter, 0, 0);
        verify(mockWriter, times(1)).keyword(",");
        verify(mockPathExpr, times(1)).unparse(mockWriter, 0, 0);
        verify(mockWriter, times(1)).keyword(")");

        // Verify COLUMNS section is not added for empty list
        verify(mockWriter, times(0)).keyword("COLUMNS");
    }

    /**
     * Test edge case with null JSON expression and path expression.
     */
    @Test
    public void testUnparseWithNullExpressions() {
        // Create SqlJsonTable with null expressions
        target = new SqlJsonTable(null, null, new ArrayList<>(), pos);

        // Test unparse with null expressions
        target.unparse(mockWriter, 0, 0);

        // Verify basic structure is maintained even with null expressions (SqlJsonTable.unparse behavior)
        verify(mockWriter, times(1)).keyword("JSON_TABLE");
        verify(mockWriter, times(1)).keyword("(");
        verify(mockWriter, times(1)).keyword(",");
        verify(mockWriter, times(1)).keyword(")");

        // Verify null expressions are not unparsed
        verify(mockJsonExpr, times(0)).unparse(any(), anyInt(), anyInt());
        verify(mockPathExpr, times(0)).unparse(any(), anyInt(), anyInt());
    }

    /**
     * Test validation with null expressions and empty columns.
     */
    @Test
    public void testValidateWithNullExpressions() {
        // Create SqlJsonTable with null expressions and empty columns
        target = new SqlJsonTable(null, null, new ArrayList<>(), pos);

        // Test validate with null expressions
        target.validate(mockValidator, mockScope);

        // Verify no validation calls are made for null expressions
        verify(mockJsonExpr, times(0)).validate(any(), any());
        verify(mockPathExpr, times(0)).validate(any(), any());
    }

    /**
     * Test basic functionality and getters.
     */
    @Test
    public void testBasicFunctionality() {
        List<SqlJsonTable.JsonTableColumn> columns = Arrays.asList(
            new SqlJsonTable.JsonTableColumn(pos, mockColumnName, mockDataType, mockColumnPath, false, false, null,
                null)
        );

        target = new SqlJsonTable(mockJsonExpr, mockPathExpr, columns, pos);

        // Test getters
        assertEquals("JSON expression should match", mockJsonExpr, target.getJsonExpr());
        assertEquals("Path expression should match", mockPathExpr, target.getPathExpr());
        assertEquals("Columns should match", columns, target.getColumns());
        assertEquals("Operator should be OPERATOR", SqlJsonTable.OPERATOR, target.getOperator());
        Assert.assertEquals("Kind should be JSON_TABLE", SqlKind.JSON_TABLE, target.getKind());

        // Test operand list
        List<SqlNode> operands = target.getOperandList();
        assertEquals("Should have 3 operands", 3, operands.size());
        assertEquals("First operand should be JSON expression", mockJsonExpr, operands.get(0));
        assertEquals("Second operand should be path expression", mockPathExpr, operands.get(1));

        // Test clone
        SqlNode cloned = target.clone(pos);
        assertNotNull("Cloned node should not be null", cloned);
        assertEquals("Cloned node should be same class", target.getClass(), cloned.getClass());
    }
}