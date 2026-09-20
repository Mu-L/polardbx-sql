/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
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
package com.alibaba.polardbx.optimizer.utils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit test for {@link SqlIdentifier#toStringWithBacktick()}.
 * <p>
 * Ensures that qualified identifiers are backtick-quoted per component,
 * producing {@code `schema`.`table`} instead of {@code `schema.table`}.
 * This is critical for CDC DDL statement parsing.
 */
public class SqlIdentifierTest {

    @Test
    public void testToStringWithBacktick_simpleName() {
        SqlIdentifier id = new SqlIdentifier(Arrays.asList("t2"), null, SqlParserPos.ZERO, null);
        assertThat(id.toStringWithBacktick(), is("`t2`"));
    }

    @Test
    public void testToStringWithBacktick_schemaTable() {
        SqlIdentifier id = new SqlIdentifier(Arrays.asList("dz1", "t2"), null, SqlParserPos.ZERO, null);
        assertThat(id.toStringWithBacktick(), is("`dz1`.`t2`"));
    }

    @Test
    public void testToStringWithBacktick_threePartName() {
        SqlIdentifier id =
            new SqlIdentifier(Arrays.asList("catalog", "schema", "table"), null, SqlParserPos.ZERO, null);
        assertThat(id.toStringWithBacktick(), is("`catalog`.`schema`.`table`"));
    }

    @Test
    public void testToStringWithBacktick_binPrefixTable() {
        SqlIdentifier id =
            new SqlIdentifier(Arrays.asList("dz1", "BIN_YPJT51HXFLXZ3ICCGJOC"), null, SqlParserPos.ZERO, null);
        assertThat(id.toStringWithBacktick(), is("`dz1`.`BIN_YPJT51HXFLXZ3ICCGJOC`"));
    }

    @Test
    public void testToStringWithBacktick_nameContainsBacktick() {
        SqlIdentifier id = new SqlIdentifier(Arrays.asList("db", "tab`le"), null, SqlParserPos.ZERO, null);
        assertThat(id.toStringWithBacktick(), is("`db`.`tab``le`"));
    }

    @Test
    public void testSurroundWithBacktick_simple() {
        assertThat(SqlIdentifier.surroundWithBacktick("abc"), is("`abc`"));
    }

    @Test
    public void testSurroundWithBacktick_withBacktick() {
        assertThat(SqlIdentifier.surroundWithBacktick("ab`c"), is("`ab``c`"));
    }

    @Test
    public void testSurroundWithBacktick_multipleBackticks() {
        assertThat(SqlIdentifier.surroundWithBacktick("a`b`c"), is("`a``b``c`"));
    }
}
