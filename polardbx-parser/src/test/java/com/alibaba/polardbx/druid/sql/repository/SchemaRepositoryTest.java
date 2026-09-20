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

package com.alibaba.polardbx.druid.sql.repository;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLIndex;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * created by ziyang.lb
 **/
public class SchemaRepositoryTest {

    public final static String DEFAULT_SCHEMA = "d`b1";
    public final static SQLParserFeature[] FEATURES = {
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache, SQLParserFeature.OptimizedForParameterized,
        SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DRDSBaseline, SQLParserFeature.DrdsMisc, SQLParserFeature.DrdsGSI, SQLParserFeature.DrdsCCL,
        SQLParserFeature.EnableFillKeyName
    };

    SchemaRepository repository;

    @Before
    public void before() {
        repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema(DEFAULT_SCHEMA);
    }

    //see Aone issue ,ID:60853701
    @Test
    public void testDropColumnWithSpace() {
        String sql = "create table if not exists `test_change_column_with_space` (\n"
            + "        `id` int not null,\n"
            + "        ` c1` int not null,\n"
            + "        `c1` int not null,\n"
            + "        primary key (`id`)\n"
            + ");";
        repository.console(sql, FEATURES);

        sql = "alter table `test_change_column_with_space` drop column ` c1`";
        repository.console(sql, FEATURES);
        SchemaObject tableMeta = findTable(repository, "test_change_column_with_space");
        Assert.assertNotNull(tableMeta.findColumn("c1"));
        Assert.assertNull(tableMeta.findColumn(" c1"));
    }

    //see Aone issue ,ID:39638018
    @Test
    public void testDropTable() {
        String sql = "create table if not exists `gxw_test``backtick`("
            + "  `col-minus` int,"
            + "  c2 int,"
            + "  _drds_implicit_id_ bigint auto_increment,"
            + "  primary key (_drds_implicit_id_)"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci";
        repository.console(sql, FEATURES);
        SchemaObject tableMeta = findTable(repository, "gxw_test`backtick");
        Assert.assertNotNull(tableMeta);

        sql = "drop table if exists `gxw_test``backtick`";
        repository.console(sql, FEATURES);
        tableMeta = findTable(repository, "gxw_test`backtick");
        Assert.assertNull(tableMeta);
    }

    //see Aone issue ,ID:39638018
    @Test
    public void testCreateIndex() {
        String sql = "create table if not exists `ng` ("
            + "        `2kkxyfni` char(1) not null comment 'kkvy',"
            + "        `i1iavmsfrvs1cpk` char(5),"
            + "        _drds_implicit_id_ bigint auto_increment,"
            + "        primary key (_drds_implicit_id_)"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci";
        repository.console(sql, FEATURES);
        SchemaObject tableMeta = findTable(repository, "ng");
        Assert.assertNotNull(tableMeta);

        sql = "create local index `ng`  on `ng` ( `feesesihp3qx`   )";
        repository.console(sql, FEATURES);
        tableMeta = findTable(repository, "ng");
        Assert.assertNotNull(tableMeta);

        tableMeta = findIndex(repository, "ng", "ng");
        Assert.assertNotNull(tableMeta);

        sql = "create local index `ab``dc`  on `ng` ( `feesesihp3qx`   )";
        repository.console(sql, FEATURES);
        tableMeta = findIndex(repository, "ng", "ab`dc");
        Assert.assertNotNull(tableMeta);

        sql = "drop index `ab``dc` on `ng`";
        repository.console(sql, FEATURES);
        tableMeta = findIndex(repository, "ng", "ab`dc");
        Assert.assertNull(tableMeta);
    }

    //see Aone issue ,ID:39638018
    @Test
    public void testRenameTable() {
        String sqlCreate = "create table if not exists `gxw_test``backtick`("
            + "  `col-minus` int,"
            + "  c2 int,"
            + "  _drds_implicit_id_ bigint auto_increment,"
            + "  primary key (_drds_implicit_id_)"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci";
        repository.console(sqlCreate, FEATURES);
        SchemaObject table1 = findTable(repository, "gxw_test`backtick");
        Assert.assertNotNull(table1);

        String sqlRename = "rename table `gxw_test``backtick` to `gxw_test``backtick_new`";
        repository.console(sqlRename, FEATURES);
        SchemaObject table2 = findTable(repository, "gxw_test`backtick");
        SchemaObject table3 = findTable(repository, "gxw_test`backtick_new");
        Assert.assertNull(table2);
        Assert.assertNotNull(table3);
    }

    @Test
    public void testAlterTable() {
        testAlterTableInternal("gxw_test``backtick", "gxw_test`backtick");
        testAlterTableInternal("``gxw_test``backtick``", "`gxw_test`backtick`");
        testAlterTableInternal("abc", "abc");
    }

    @Test
    public void testAlterTableCharset() {
        String ddl = "CREATE TABLE IF NOT EXISTS `all_type`\n"
            + "(`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "`c_bit_1` bit(1) DEFAULT b'1',\n"
            + "`c_bit_8` bit(8) DEFAULT b'11111111',\n"
            + "`c_bit_16` bit(16) DEFAULT b'1111111111111111',\n"
            + "`c_bit_32` bit(32) DEFAULT b'11111111111111111111111111111111',\n"
            + "`c_bit_64` bit(64) DEFAULT b'1111111111111111111111111111111111111111111111111111111111111111',\n"
            + "`c_bit_hex_8` bit(8) DEFAULT 0xFF,\n"
            + "`c_bit_hex_16` bit(16) DEFAULT 0xFFFF,\n"
            + "`c_bit_hex_32` bit(32) DEFAULT 0xFFFFFFFF,\n"
            + "`c_bit_hex_64` bit(64) DEFAULT 0xFFFFFFFFFFFFFFFF,\n"
            + "`c_boolean` tinyint(1) DEFAULT true,\n"
            + "`c_boolean_2` tinyint(1) DEFAULT false,\n"
            + "`c_boolean_3` boolean DEFAULT false,\n"
            + "`c_tinyint_1` tinyint(1) DEFAULT 127,\n"
            + "`c_tinyint_4` tinyint(4) DEFAULT -128,\n"
            + "`c_tinyint_8` tinyint(8) DEFAULT -75,\n"
            + "`c_tinyint_3_un` tinyint(3) UNSIGNED DEFAULT 0,\n"
            + "`c_tinyint_8_un` tinyint(8) unsigned DEFAULT 255,\n"
            + "`c_tinyint_df_un` tinyint unsigned DEFAULT 255,\n"
            + "`c_tinyint_zerofill_un` tinyint(3) UNSIGNED ZEROFILL DEFAULT 255,\n"
            + "`c_smallint_1` smallint(1) DEFAULT 14497,\n"
            + "`c_smallint_2` smallint(2) DEFAULT 111,\n"
            + "`c_smallint_6` smallint(6) DEFAULT -32768,\n"
            + "`c_smallint_16` smallint(16) DEFAULT 32767,\n"
            + "`c_smallint_16_un` smallint(16) unsigned DEFAULT 65535,\n"
            + "`c_smallint_df_un` smallint unsigned DEFAULT 65535,\n"
            + "`c_smallint_zerofill_un` smallint(5) UNSIGNED ZEROFILL DEFAULT 65535,\n"
            + "`c_mediumint_1` mediumint(1) DEFAULT -8388608,\n"
            + "`c_mediumint_3` mediumint(3) DEFAULT 3456789,\n"
            + "`c_mediumint_9` mediumint(9) DEFAULT 8388607,\n"
            + "`c_mediumint_24` mediumint(24) DEFAULT -1845105,\n"
            + "`c_mediumint_8_un` mediumint(8) UNSIGNED DEFAULT 16777215,\n"
            + "`c_mediumint_24_un` mediumint(24) unsigned DEFAULT 16777215,\n"
            + "`c_mediumint_df_un` mediumint unsigned DEFAULT 16777215,\n"
            + "`c_mediumint_zerofill_un` mediumint(8) UNSIGNED ZEROFILL DEFAULT 7788,\n"
            + "`c_int_1` int(1) DEFAULT -2147483648,\n"
            + "`c_int_4` int(4) DEFAULT 872837,\n"
            + "`c_int_11` int(11) DEFAULT 2147483647,\n"
            + "`c_int_32` int(32) DEFAULT -2147483648,\n"
            + "`c_int_32_un` int(32) unsigned DEFAULT 4294967295,\n"
            + "`c_int_df_un` int unsigned DEFAULT 4294967295,\n"
            + "`c_int_zerofill_un` int(10) UNSIGNED ZEROFILL DEFAULT 4294967295,\n"
            + "`c_bigint_1` bigint(1) DEFAULT -816854218224922624,\n"
            + "`c_bigint_20` bigint(20) DEFAULT -9223372036854775808,\n"
            + "`c_bigint_64` bigint(64) DEFAULT 9223372036854775807,\n"
            + "`c_bigint_20_un` bigint(20) UNSIGNED DEFAULT 9223372036854775808,\n"
            + "`c_bigint_64_un` bigint(64) unsigned DEFAULT 18446744073709551615,\n"
            + "`c_bigint_df_un` bigint unsigned DEFAULT 18446744073709551615,\n"
            + "`c_bigint_zerofill_un` bigint(20) UNSIGNED ZEROFILL DEFAULT 1,\n"
            + "`c_tinyint_hex_1` tinyint(1) DEFAULT 0x3F,\n"
            + "`c_tinyint_hex_4` tinyint(4) DEFAULT 0x4F,\n"
            + "`c_tinyint_hex_8` tinyint(8) DEFAULT 0x5F,\n"
            + "`c_tinyint_hex_3_un` tinyint(3) UNSIGNED DEFAULT 0x2F,\n"
            + "`c_tinyint_hex_8_un` tinyint(8) unsigned DEFAULT 0x4E,\n"
            + "`c_tinyint_hex_zerofill_un` tinyint(3) UNSIGNED ZEROFILL DEFAULT 0x3F,\n"
            + "`c_smallint_hex_1` smallint(1) DEFAULT 0x2FFF,\n"
            + "`c_smallint_hex_2` smallint(2) DEFAULT 0x3FFF,\n"
            + "`c_smallint_hex_6` smallint(6) DEFAULT 0x4FEF,\n"
            + "`c_smallint_hex_16` smallint(16) DEFAULT 0x5EDF,\n"
            + "`c_smallint_hex_16_un` smallint(16) unsigned DEFAULT 0x7EDF,\n"
            + "`c_smallint_hex_zerofill_un` smallint(5) UNSIGNED ZEROFILL DEFAULT 0x8EFF,\n"
            + "`c_mediumint_hex_1` mediumint(1) DEFAULT 0x9EEE,\n"
            + "`c_mediumint_hex_3` mediumint(3) DEFAULT 0x7DDD,\n"
            + "`c_mediumint_hex_9` mediumint(9) DEFAULT 0x6CCC,\n"
            + "`c_mediumint_hex_24` mediumint(24) DEFAULT 0x5FCC,\n"
            + "`c_mediumint_hex_8_un` mediumint(8) UNSIGNED DEFAULT 0xFCFF,\n"
            + "`c_mediumint_hex_24_un` mediumint(24) unsigned DEFAULT 0xFCFF,\n"
            + "`c_mediumint_hex_zerofill_un` mediumint(8) UNSIGNED ZEROFILL DEFAULT 0xFFFF,\n"
            + "`c_int_hex_1` int(1) DEFAULT 0xFFFFFF,\n"
            + "`c_int_hex_4` int(4) DEFAULT 0xEFFFFF,\n"
            + "`c_int_hex_11` int(11) DEFAULT 0xEEFFFF,\n"
            + "`c_int_hex_32` int(32) DEFAULT 0xEEFFFF,\n"
            + "`c_int_hex_32_un` int(32) unsigned DEFAULT 0xFFEEFF,\n"
            + "`c_int_hex_zerofill_un` int(10) UNSIGNED ZEROFILL DEFAULT 0xFFEEFF,\n"
            + "`c_bigint_hex_1` bigint(1) DEFAULT 0xFEFFFFFFFEFFFF,\n"
            + "`c_bigint_hex_20` bigint(20) DEFAULT 0xFFFFFFFFFEFFFF,\n"
            + "`c_bigint_hex_64` bigint(64) DEFAULT 0xEFFFFFFFFEFFFF,\n"
            + "`c_bigint_hex_20_un` bigint(20) UNSIGNED DEFAULT 0xCFFFFFFFFEFFFF,\n"
            + "`c_bigint_hex_64_un` bigint(64) unsigned DEFAULT 0xAFFFFFFFFEFFFF,\n"
            + "`c_bigint_hex_zerofill_un` bigint(20) UNSIGNED ZEROFILL DEFAULT 0x1,\n"
            + "`c_decimal_hex` decimal DEFAULT 0xFFFFFF,\n"
            + "`c_decimal_hex_pr` decimal(10,3) DEFAULT 0xEFFF,\n"
            + "`c_decimal_hex_un` decimal(10,0) UNSIGNED DEFAULT 0xFFFF,\n"
            + "`c_float_hex` float DEFAULT 0xEFFF,\n"
            + "`c_float_hex_pr` float(10,3) DEFAULT 0xEEEE,\n"
            + "`c_float_hex_un` float(10,3) unsigned DEFAULT 0xFFEF,\n"
            + "`c_double_hex` double DEFAULT 0xFFFFEFFF,\n"
            + "`c_double_hex_pr` double(10,3) DEFAULT 0xFFFF,\n"
            + "`c_double_hex_un` double(10,3) unsigned DEFAULT 0xFFFF,\n"
            + "`c_tinyint_hex_x_1` tinyint(1) DEFAULT x'1F',\n"
            + "`c_tinyint_hex_x_4` tinyint(4) DEFAULT x'2F',\n"
            + "`c_tinyint_hex_x_8` tinyint(8) DEFAULT x'3F',\n"
            + "`c_tinyint_hex_x_3_un` tinyint(3) UNSIGNED DEFAULT x'FF',\n"
            + "`c_tinyint_hex_x_8_un` tinyint(8) unsigned DEFAULT x'EE',\n"
            + "`c_tinyint_hex_x_zerofill_un` tinyint(3) UNSIGNED ZEROFILL DEFAULT x'FF',\n"
            + "`c_smallint_hex_x_1` smallint(1) DEFAULT x'1FFF',\n"
            + "`c_smallint_hex_x_2` smallint(2) DEFAULT x'1FFF',\n"
            + "`c_smallint_hex_x_6` smallint(6) DEFAULT x'2FEF',\n"
            + "`c_smallint_hex_x_16` smallint(16) DEFAULT x'1EDF',\n"
            + "`c_smallint_hex_x_16_un` smallint(16) unsigned DEFAULT x'1EDF',\n"
            + "`c_smallint_hex_x_zerofill_un` smallint(5) UNSIGNED ZEROFILL DEFAULT x'5EFF',\n"
            + "`c_mediumint_hex_x_1` mediumint(1) DEFAULT x'4EEE',\n"
            + "`c_mediumint_hex_x_3` mediumint(3) DEFAULT x'3DDD',\n"
            + "`c_mediumint_hex_x_9` mediumint(9) DEFAULT x'2CCC',\n"
            + "`c_mediumint_hex_x_24` mediumint(24) DEFAULT x'1FCC',\n"
            + "`c_mediumint_hex_x_8_un` mediumint(8) UNSIGNED DEFAULT x'FAFF',\n"
            + "`c_mediumint_hex_x_24_un` mediumint(24) unsigned DEFAULT x'FAFF',\n"
            + "`c_mediumint_hex_x_zerofill_un` mediumint(8) UNSIGNED ZEROFILL DEFAULT x'FAFF',\n"
            + "`c_int_hex_x_1` int(1) DEFAULT x'FFFFFF',\n"
            + "`c_int_hex_x_4` int(4) DEFAULT x'FFFFFF',\n"
            + "`c_int_hex_x_11` int(11) DEFAULT x'FFFFFF',\n"
            + "`c_int_hex_x_32` int(32) DEFAULT x'FFFFFF',\n"
            + "`c_int_hex_x_32_un` int(32) unsigned DEFAULT x'FFFFFF',\n"
            + "`c_int_hex_x_zerofill_un` int(10) UNSIGNED ZEROFILL DEFAULT x'FFFFFF',\n"
            + "`c_bigint_hex_x_1` bigint(1) DEFAULT x'FFFFFFFFFFFFFF',\n"
            + "`c_bigint_hex_x_20` bigint(20) DEFAULT x'FFFFFFFFFFFFFF',\n"
            + "`c_bigint_hex_x_64` bigint(64) DEFAULT x'FFFFFFFFFFFFFF',\n"
            + "`c_bigint_hex_x_20_un` bigint(20) UNSIGNED DEFAULT x'FFFFFFFFFFFFFF',\n"
            + "`c_bigint_hex_x_64_un` bigint(64) unsigned DEFAULT x'FFFFFFFFFFFFFF',\n"
            + "`c_bigint_hex_x_zerofill_un` bigint(20) UNSIGNED ZEROFILL DEFAULT x'F1AB',\n"
            + "`c_decimal_hex_x` decimal DEFAULT x'FFFFFFFF',\n"
            + "`c_decimal_hex_x_pr` decimal(10,3) DEFAULT x'FFFF',\n"
            + "`c_decimal_hex_x_un` decimal(10,0) UNSIGNED DEFAULT x'FFFF',\n"
            + "`c_float_hex_x` float DEFAULT x'FFFF',\n"
            + "`c_float_hex_x_pr` float(10,3) DEFAULT x'EEEE',\n"
            + "`c_float_hex_x_un` float(10,3) unsigned DEFAULT x'FFFF',\n"
            + "`c_double_hex_x` double DEFAULT x'FFFFEFFF',\n"
            + "`c_double_hex_x_pr` double(10,3) DEFAULT x'FFFF',\n"
            + "`c_double_hex_x_un` double(10,3) unsigned DEFAULT x'FFFF',\n"
            + "`c_decimal` decimal DEFAULT -1613793319,\n"
            + "`c_decimal_pr` decimal(10,3) DEFAULT 1223077.292,\n"
            + "`c_decimal_un` decimal(10,0) UNSIGNED DEFAULT 10234273,\n"
            + "`c_numeric_df` numeric DEFAULT 10234273,\n"
            + "`c_numeric_10` numeric(10,5) DEFAULT 1,\n"
            + "`c_numeric_df_un` numeric UNSIGNED DEFAULT 10234273,\n"
            + "`c_numeric_un` numeric(10,6) UNSIGNED DEFAULT 1,\n"
            + "`c_dec_df` dec DEFAULT 10234273,\n"
            + "`c_dec_10` dec(10,5) DEFAULT 1,\n"
            + "`c_dec_df_un` dec UNSIGNED DEFAULT 10234273,\n"
            + "`c_dec_un` dec(10,6) UNSIGNED DEFAULT 1,\n"
            + "`c_float` float DEFAULT 9.1096275E8,\n"
            + "`c_float_pr` float(10,3) DEFAULT -5839673.5,\n"
            + "`c_float_un` float(10,3) unsigned DEFAULT 2648.644,\n"
            + "`c_double` double DEFAULT 4.334081673614155E9,\n"
            + "`c_double_pr` double(10,3) DEFAULT 6973286.176,\n"
            + "`c_double_un` double(10,3) unsigned DEFAULT 7630560.182,\n"
            + "`c_date` date DEFAULT '2019-02-15' COMMENT 'date',\n"
            + "`c_datetime` datetime DEFAULT '2019-02-15 14:54:41',\n"
            + "`c_datetime_ms` datetime(3) DEFAULT '2019-02-15 14:54:41.789',\n"
            + "`c_datetime_df` datetime DEFAULT CURRENT_TIMESTAMP,\n"
            + "`c_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
            + "`c_timestamp_2` timestamp DEFAULT '2020-12-29 12:27:30',\n"
            + "`c_time` time DEFAULT '20:12:46',\n"
            + "`c_time_3` time(3) DEFAULT '12:30',\n"
            + "`c_year` year DEFAULT '2019',\n"
            + "`c_year_4` year(4) DEFAULT '2029',\n"
            + "`c_char` char(50) DEFAULT 'sjdlfjsdljldfjsldfsd',\n"
            + "`c_char_df` char DEFAULT 'x',\n"
            + "`c_varchar` varchar(50) DEFAULT 'sjdlfjsldhgowuere',\n"
            + "`c_nchar` nchar(100) DEFAULT '你好',\n"
            + "`c_nvarchar` nvarchar(100) DEFAULT '北京',\n"
            + "`c_binary_df` binary DEFAULT 'x',\n"
            + "`c_binary` binary(200) DEFAULT 'qoeuroieshdfs',\n"
            + "`c_varbinary` varbinary(200) DEFAULT 'sdfjsljlewwfs',\n"
            + "`c_blob_tiny` tinyblob DEFAULT NULL,\n"
            + "`c_blob` blob DEFAULT NULL,\n"
            + "`c_blob_medium` mediumblob DEFAULT NULL,\n"
            + "`c_blob_long` longblob DEFAULT NULL,\n"
            + "`c_text_tiny` tinytext DEFAULT NULL,\n"
            + "`c_text` text DEFAULT NULL,\n"
            + "`c_text_medium` mediumtext DEFAULT NULL,\n"
            + "`c_text_long` longtext DEFAULT NULL,\n"
            + "`c_enum` enum('a','b','c') DEFAULT 'a',\n"
            + "`c_enum_2` enum('x-small', 'small', 'medium', 'large', 'x-large') DEFAULT 'small',\n"
            + "`c_set` set('a','b','c') DEFAULT 'a',\n"
            + "`c_json` json DEFAULT NULL,\n"
            + "`c_geo` geometry DEFAULT NULL,`c_idx` bigint not null default 100,PRIMARY KEY (`id`)) default charset gbk";

        String alter1 = "ALTER TABLE `all_type`\n"
            + "        DEFAULT CHARACTER SET=utf8";
        repository.console(ddl, FEATURES);

        SchemaObject table1 = findTable(repository, "all_type");
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) table1.getStatement();
        List<SQLAssignItem> itemList = stmt.getTableOptions();
        String charset = null;
        for (SQLAssignItem item : itemList) {
            if (item.getTarget().toString().contains("CHARSET") ||
                item.getTarget().toString().contains("CHARACTER")) {
                charset = item.getValue().toString();
            }
        }
        Assert.assertEquals(charset, "gbk");

        repository.console(alter1, FEATURES);
        SchemaObject table2 = findTable(repository, "all_type");
        MySqlCreateTableStatement stmt2 = (MySqlCreateTableStatement) table2.getStatement();
        for (SQLColumnDefinition definition : stmt2.getColumnDefinitions()) {
            String dataTypeName = definition.getDataType().getName();
            if (StringUtils.containsIgnoreCase(dataTypeName, "text") || StringUtils.equalsIgnoreCase(dataTypeName,
                "char") || StringUtils.equalsIgnoreCase(dataTypeName,
                "varchar") || StringUtils.equalsIgnoreCase(dataTypeName,
                "enum") || StringUtils.equalsIgnoreCase(dataTypeName,
                "set")) {
                Assert.assertEquals(definition.getCharsetExpr().toString(), "gbk");
            } else {
                Assert.assertNull(definition.getCharsetExpr());
            }
        }
        itemList = stmt2.getTableOptions();
        charset = null;
        for (SQLAssignItem item : itemList) {
            if (item.getTarget().toString().contains("CHARSET") ||
                item.getTarget().toString().contains("CHARACTER")) {
                charset = item.getValue().toString();
            }
        }
        Assert.assertEquals(charset, "utf8");

        repository.console("alter table all_type convert to character set utf8mb4", FEATURES);
        SchemaObject table3 = findTable(repository, "all_type");
        MySqlCreateTableStatement stmt3 = (MySqlCreateTableStatement) table3.getStatement();
        Assert.assertEquals("CHAR", stmt3.findColumn("c_nchar").getDataType().getName());
        Assert.assertEquals("VARCHAR", stmt3.findColumn("c_nvarchar").getDataType().getName());
        Assert.assertEquals("TEXT", stmt3.findColumn("c_text_tiny").getDataType().getName());
        Assert.assertEquals("MEDIUMTEXT", stmt3.findColumn("c_text").getDataType().getName());
        Assert.assertEquals("LONGTEXT", stmt3.findColumn("c_text_medium").getDataType().getName());
        for (SQLColumnDefinition definition : stmt3.getColumnDefinitions()) {
            String dataTypeName = definition.getDataType().getName();
            if (StringUtils.containsIgnoreCase(dataTypeName, "text") || StringUtils.equalsIgnoreCase(dataTypeName,
                "char") || StringUtils.equalsIgnoreCase(dataTypeName,
                "varchar") || StringUtils.equalsIgnoreCase(dataTypeName,
                "enum") || StringUtils.equalsIgnoreCase(dataTypeName,
                "set")) {
                Assert.assertEquals(definition.getCharsetExpr().toString(), "utf8mb4");
            } else {
                Assert.assertNull(definition.getCharsetExpr());
            }
        }
    }

    @Test
    public void testAlgorithm() {
        String sql1 =
            "create table `omc_not_null_tbl_test_w3za_00001` ( \ta int primary key, \tb int not null ) dbpartition by hash(a)";
        String sql2 = "alter table `omc_not_null_tbl_test_w3za_00001` \tadd column `b_ehjp` int after `b`, "
            + "\talgorithm = default";
        repository.console(sql1, FEATURES);
        repository.console(sql2, FEATURES);
        SchemaObject table1 = findTable(repository, "omc_not_null_tbl_test_w3za_00001");
        SQLStatement statement = table1.getStatement();
        String sql3 = statement.toString();
        repository.console(sql3, FEATURES);
        checkSql(sql3);
    }

    @Test
    public void testCreateSequence() {
        String sql = "create sequence pxc_seq_64056c9e413d6f79544c4938f86c8d6e start with 1 cache 100000";
        repository.console(sql, FEATURES);
        SchemaObject object = findSequence(repository, "pxc_seq_64056c9e413d6f79544c4938f86c8d6e");
        Assert.assertEquals("CREATE SEQUENCE pxc_seq_64056c9e413d6f79544c4938f86c8d6e START WITH 1 CACHE 100000",
            object.getStatement().toString());
    }

    @Test
    public void testDropIndex() {
        repository.console("create table test_escaping_col_name (\n"
            + "  id int not null primary key,\n"
            + "  name varchar(10),\n"
            + "  age int,\n"
            + "  dept int\n"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci");

        repository.console("create index idx_age on test_escaping_col_name(age)");
        Set<String> sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age"), sets);

        repository.console("alter table test_escaping_col_name add index idx_dept(`dept`)");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age", "idx_dept"), sets);

        repository.console("alter table test_escaping_col_name drop index idx_dept");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age"), sets);

        repository.console("drop index idx_age on test_escaping_col_name");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet(), sets);
    }

    //See aone issue, id : 46539884
    @Test
    public void testConstraintName() {
        String sql1 = " create table `t_test` (\n"
            + "        `id` bigint(20) unsigned not null auto_increment,\n"
            + "        `a` bigint(20) not null comment '',\n"
            + "        `b` bigint(20) not null comment '',\n"
            + "        `c` tinyint(4) not null comment '',\n"
            + "        `d` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `e` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `f` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `g` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `h` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `i` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `j` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `k` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `l` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `m` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `n` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `o` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `p` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `q` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `r` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `s` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `t` timestamp not null default current_timestamp,\n"
            + "        `e` timestamp not null default current_timestamp on update current_timestamp,\n"
            + "        primary key (`id`),\n"
            + "        unique key `uk_x` (`a`, `b`, `c`),\n"
            + "        constraint unique `uk_xx` (`a`, `b`, `c`)\n"
            + ")";
        String sql2 = "alter table t_test add constraint `uk_y` unique (e, f, g, h)";
        String sql3 = "alter table t_test drop constraint uk_y";

        String sql4 = "alter table t_test add constraint `uk_h` unique (e, f, g, h)";
        String sql5 = "alter table t_test drop index uk_h";

        String sql6 = "alter table t_test add unique key uk_o(o,p,q)";
        String sql7 = "alter table t_test drop constraint uk_o";

        String sql8 = "alter table t_test add constraint `uk_f` unique (e, f, g, h)";
        String sql9 = "create index idx_z on t_test (g,h ,i ,j ,k ,l,m ,n)";
        String sql10 = "drop index idx_z on t_test";

        String sql11 = "alter table t_test drop primary key";
        String sql12 = "alter table t_test add constraint primary key(id)";

        repository.console(sql1);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx"), findIndexes(DEFAULT_SCHEMA, "t_test"));

        repository.console(sql2);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_y"), findIndexes(DEFAULT_SCHEMA, "t_test"));
        repository.console(sql3);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx"), findIndexes(DEFAULT_SCHEMA, "t_test"));

        repository.console(sql4);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_h"), findIndexes(DEFAULT_SCHEMA, "t_test"));
        repository.console(sql5);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx"), findIndexes(DEFAULT_SCHEMA, "t_test"));

        repository.console(sql6);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_o"), findIndexes(DEFAULT_SCHEMA, "t_test"));
        repository.console(sql7);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx"), findIndexes(DEFAULT_SCHEMA, "t_test"));

        repository.console(sql8);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f"), findIndexes(DEFAULT_SCHEMA, "t_test"));
        repository.console(sql9);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f", "idx_z"), findIndexes(DEFAULT_SCHEMA, "t_test"));
        repository.console(sql10);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f"), findIndexes(DEFAULT_SCHEMA, "t_test"));

        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f", "primary"),
            findIndexes(DEFAULT_SCHEMA, "t_test", true));
        repository.console(sql11);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f"),
            findIndexes(DEFAULT_SCHEMA, "t_test", true));
        repository.console(sql12);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f", "primary"),
            findIndexes(DEFAULT_SCHEMA, "t_test", true));
    }

    @Test
    public void testUniqueKeyInColumn() {
        String sql1 = "CREATE TABLE IF NOT EXISTS `ZXe5GA6` (\n"
            + "  `aiMzgbaKVCIQtle` INT(1) UNSIGNED NULL COMMENT 'treSay',\n"
            + "  `V8R9mZFvUHxQ` MEDIUMINT UNSIGNED ZEROFILL COMMENT 'WenHosxfI3i',\n"
            + "  `RFKRrCAF` TIMESTAMP UNIQUE,\n"
            + "  `ctv` BIGINT(5) ZEROFILL NULL,\n"
            + "  `Vd` TINYINT UNSIGNED ZEROFILL UNIQUE COMMENT 'lysAE',\n"
            + "  `8` MEDIUMINT(4) ZEROFILL COMMENT 'Y',\n"
            + "  `H4rJ5c8d0N1C8Q` BIGINT UNSIGNED ZEROFILL NOT NULL,\n"
            + "  `iE69EIYRLOqXa3` DATE NOT NULL COMMENT 'VAHhex',\n"
            + "  `OsBUdkS` MEDIUMINT ZEROFILL COMMENT 'zgV7ojRAJKgu4XI',\n"
            + "  `LADuM` TIMESTAMP(0) COMMENT 'nkaLg0',\n"
            + "  `kO38Dx6gYUPRtBn` MEDIUMINT UNSIGNED ZEROFILL UNIQUE,\n"
            + "  KEY `Cb` USING HASH (`Vd`),\n"
            + "  INDEX `auto_shard_key_ctv` USING BTREE(`CTV`),\n"
            + "  INDEX `auto_shard_key_ie69eiyrloqxa3` USING BTREE(`IE69EIYRLOQXA3`),\n"
            + "  _drds_implicit_id_ bigint AUTO_INCREMENT,\n"
            + "  PRIMARY KEY (_drds_implicit_id_)\n"
            + ")\n"
            + "DBPARTITION BY RIGHT_SHIFT(`ctv`, 9)\n"
            + "TBPARTITION BY YYYYMM(`iE69EIYRLOqXa3`) TBPARTITIONS 7";
        String sql2 = "DROP INDEX `ko38dx6gyuprtbn` ON `ZXe5GA6`";
        String sql3 = "alter table `ZXe5GA6` drop index RFKRrCAF";

        repository.console(sql1, FEATURES);
        Assert.assertEquals(Sets.newHashSet("kO38Dx6gYUPRtBn", "auto_shard_key_ie69eiyrloqxa3",
            "RFKRrCAF", "Vd", "auto_shard_key_ctv", "Cb"), findIndexes(DEFAULT_SCHEMA, "ZXe5GA6"));

        repository.console(sql2, FEATURES);
        Assert.assertEquals(Sets.newHashSet("auto_shard_key_ie69eiyrloqxa3", "RFKRrCAF",
            "Vd", "auto_shard_key_ctv", "Cb"), findIndexes(DEFAULT_SCHEMA, "ZXe5GA6"));

        repository.console(sql3, FEATURES);
        Assert.assertEquals(Sets.newHashSet("auto_shard_key_ie69eiyrloqxa3", "Vd", "auto_shard_key_ctv",
            "Cb"), findIndexes(DEFAULT_SCHEMA, "ZXe5GA6"));
    }

    @Test
    public void testRenameIndex() {
        repository.console("create table test_escaping_col_name (\n"
            + "  id int not null primary key,\n"
            + "  name varchar(10),\n"
            + "  age int,\n"
            + "  dept int\n"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci");

        repository.console("create index idx_age on test_escaping_col_name(age)");
        Set<String> sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age"), sets);

        repository.console("alter table test_escaping_col_name add index idx_dept(`dept`)");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age", "idx_dept"), sets);

        repository.console("alter table test_escaping_col_name rename index idx_dept to `bbb`");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age", "bbb"), sets);

        repository.console("alter table test_escaping_col_name rename index idx_age to `'`");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("bbb", "'"), sets);
    }

    @Test
    public void testLocalPartition() {
        String ddl = " CREATE TABLE `t_xxx` (\n"
            + "        `id` bigint(20) NOT NULL DEFAULT '0' COMMENT '',\n"
            + "        `dksl` varchar(36) NOT NULL DEFAULT '' COMMENT '',\n"
            + "        `dlsc` varchar(36) NOT NULL DEFAULT '' COMMENT '',\n"
            + "        `chw` smallint(6) NOT NULL DEFAULT '0' COMMENT '',\n"
            + "        `co2o` varchar(5000) NOT NULL DEFAULT '' COMMENT '',\n"
            + "        `cnx` varchar(200) NOT NULL DEFAULT '' COMMENT '',\n"
            + "        `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '',\n"
            + "        `dow` varchar(36) NOT NULL DEFAULT '' COMMENT '',\n"
            + "        PRIMARY KEY USING BTREE (`id`, `create_time`),\n"
            + "        LOCAL KEY `_local_idx_xdfd` USING BTREE (`dksl`) COMMENT '',\n"
            + "        LOCAL KEY `_local_idx_kdfs` USING BTREE (`create_time`) COMMENT ''\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 ROW_FORMAT = COMPACT COMMENT ''\n"
            + "LOCAL PARTITION BY RANGE (create_time)\n"
            + "STARTWITH '2022-01-01'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 3\n"
            + "PIVOTDATE NOW()\n"
            + "DISABLE SCHEDULE\u0000";
        repository.console(ddl);
    }

    @Test
    public void testCreateViewClone() {
        String sql =
            " create or replace algorithm=undefined definer=`admin`@`%` sql security definer view `v`(`c1`) as select a.pk from select_base_two_one_db_one_tb a join select_base_three_multi_db_one_tb b on a.pk = b.p";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql, FEATURES);
        StringBuffer sb = new StringBuffer();
        stmtList.get(0).clone().output(sb);
        System.out.println(sb);
    }

    @Test
    public void testAlterView() {
        String sql =
            "alter algorithm=undefined definer=`admin`@`%` sql security definer view `v2_unrelated` as select 2 as r1";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql, FEATURES);
        stmtList.get(0);
    }

    @Test
    public void testDropPrimaryKey() {
        String tableName = "t";
        String createTable = " create table t(id bigint primary key , name varchar(20) UNIQUE, content text)";
        repository.console(createTable);
        SchemaObject table = repository.findTable(tableName);
        SQLColumnDefinition idDefine = table.findColumn("id");
        Assert.assertTrue(idDefine.isPrimaryKey());
        String alterSql = "alter table t drop primary key";
        repository.console(alterSql);
        table = repository.findTable(tableName);
        idDefine = table.findColumn("id");
        Assert.assertFalse(idDefine.isPrimaryKey());
    }

    @Test
    public void testDropPrimaryKey2() {
        String tableName = "t";
        String createTable = " create table t(id bigint , name varchar(20) UNIQUE, content text, primary key(id))";
        repository.console(createTable);
        SchemaObject table = repository.findTable(tableName);
        SQLColumnDefinition idDefine = table.findColumn("id");
        Assert.assertTrue(idDefine.isPrimaryKey());
        String alterSql = "alter table t drop primary key";
        repository.console(alterSql);
        table = repository.findTable(tableName);
        idDefine = table.findColumn("id");
        Assert.assertFalse(idDefine.isPrimaryKey());
    }

    //@see : https://aone.alibaba-inc.com/v2/project/860366/bug/52107715
    @Test
    public void testAutoGeneratNameForConstraint() {
        String sql1 = "CREATE TABLE t_test_key (\n"
            + "\tid bigint NOT NULL AUTO_INCREMENT,\n"
            + "\ta bigint unique,\n"
            + "\tb int,\n"
            + "\tgmt_modified DATE NOT NULL,\n"
            + "\tkey (a),\n"
            + "\tkey a_3(a),\n"
            + "\tkey (a),\n"
            + "\tkey a_100(a),\n"
            + "\tkey (a),\n"
            + "\tunique key (a),\n"
            + "\tunique key (b),\n"
            + "\tunique key (b),\n"
            + "\tindex (b),\n"
            + "\tindex (b),\n"
            + "\tindex (b),\n"
            + "\tPRIMARY KEY (id, gmt_modified)\n"
            + ") DEFAULT CHARACTER SET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci";

        System.out.println("create sql input \n" + sql1);
        repository.console(sql1, FEATURES);
        MySqlCreateTableStatement mySqlCreateTableStatement =
            (MySqlCreateTableStatement) findTable(repository, "t_test_key").getStatement();
        System.out.println("create sql output \n" + mySqlCreateTableStatement.toString());

        String expected = "CREATE TABLE t_test_key (\n"
            + "\tid bigint NOT NULL AUTO_INCREMENT,\n"
            + "\ta bigint UNIQUE,\n"
            + "\tb int,\n"
            + "\tgmt_modified DATE NOT NULL,\n"
            + "\tKEY a_2 (a),\n"
            + "\tKEY a_3 (a),\n"
            + "\tKEY a_4 (a),\n"
            + "\tKEY a_100 (a),\n"
            + "\tKEY a_5 (a),\n"
            + "\tUNIQUE KEY a_6 (a),\n"
            + "\tUNIQUE KEY b (b),\n"
            + "\tUNIQUE KEY b_2 (b),\n"
            + "\tINDEX b_3(b),\n"
            + "\tINDEX b_4(b),\n"
            + "\tINDEX b_5(b),\n"
            + "\tPRIMARY KEY (id, gmt_modified)\n"
            + ") DEFAULT CHARACTER SET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci";
        Assert.assertEquals(expected, mySqlCreateTableStatement.toString());
    }

    @Test
    public void testCreateTableLike() {
        String t1 = "`gsi-`test_table`";
        String t2 = "`gsi-`test_table`_0h4o_009";
        String t3 = "`gsi-`test_table`_0h4o_110";
        String createSql = String.format("create table %s (id bigint primary key)", "`" + escape(t1) + "`");
        String createLikeSql = buildCreateLikeSql(t2, DEFAULT_SCHEMA, t1);

        repository.console(createSql);
        SchemaObject schemaObject = findTable(repository, t1);
        Assert.assertNotNull(schemaObject);
        SQLCreateTableStatement createTableStatement = (SQLCreateTableStatement) schemaObject.getStatement();
        Assert.assertFalse(createTableStatement.getTableElementList().isEmpty());

        repository.console(createLikeSql);
        schemaObject = findTable(repository, t2);
        Assert.assertNotNull(schemaObject);
        createTableStatement = (SQLCreateTableStatement) schemaObject.getStatement();
        Assert.assertFalse(createTableStatement.getTableElementList().isEmpty());

        createLikeSql = buildCreateLikeSql(t3, null, t1);
        repository.console(createLikeSql);
        schemaObject = findTable(repository, t3);
        Assert.assertNotNull(schemaObject);
        createTableStatement = (SQLCreateTableStatement) schemaObject.getStatement();
        Assert.assertFalse(createTableStatement.getTableElementList().isEmpty());
    }

    private String escape(String str) {
        String regex = "(?<!`)`(?!`)";
        return str.replaceAll(regex, "``");
    }

    private String buildCreateLikeSql(String tableName, String baseSchemaName, String baseTableName) {
        if (StringUtils.isNotBlank(baseSchemaName)) {
            return "create table `" + escape(tableName) + "` like `" +
                escape(baseSchemaName) + "`.`" + escape(baseTableName) + "`";
        } else {
            return "create table `" + escape(tableName) + "` like `" + escape(baseTableName) + "`";
        }
    }

    private void testAlterTableInternal(String tableName1, String tableName2) {
        String sql1 = "create table if not exists `" + tableName1 + "` ("
            + " `col-minus` int, "
            + " c2 int, "
            + " _drds_implicit_id_ bigint auto_increment, "
            + " primary key (_drds_implicit_id_)"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci";
        String sql2 = "alter table `" + tableName1 + "` add c3 int";

        repository.console(sql1, FEATURES);
        repository.console(sql2, FEATURES);

        SchemaObject table = findTable(repository, tableName2);
        SQLColumnDefinition columnDefinition1 = table.findColumn("c2");
        Assert.assertNotNull(columnDefinition1);

        SQLColumnDefinition columnDefinition2 = table.findColumn("c3");
        Assert.assertNotNull(columnDefinition2);
    }

    private static void checkSql(String sql) {
        List<SQLStatement> phyStatementList =
            SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES).parseStatementList();
        phyStatementList.get(0);
    }

    private SchemaObject findTable(SchemaRepository repository, String tableName) {
        Schema schema = repository.findSchema("d`b1");
        return schema.findTable(tableName);
    }

    private SchemaObject findIndex(SchemaRepository repository, String tableName, String indexName) {
        Schema schema = repository.findSchema("d`b1");
        return schema.getStore().getIndex(FnvHash.hashCode64(tableName + "." + indexName));
    }

    public Set<String> findIndexes(String schema, String table) {
        return findIndexes(schema, table, false);
    }

    public Set<String> findIndexes(String schema, String table, boolean includePrimary) {
        Set<String> result = new HashSet<>();

        Schema schemaRep = repository.findSchema(schema);
        if (schemaRep == null) {
            return result;
        }

        SchemaObject data = schemaRep.findTable(table);
        if (data == null) {
            return result;
        }

        SQLStatement statement = data.getStatement();
        if (statement == null) {
            return result;
        }

        if (statement instanceof SQLCreateTableStatement) {
            SQLCreateTableStatement sqlCreateTableStatement = (SQLCreateTableStatement) statement;
            sqlCreateTableStatement.getTableElementList().forEach(e -> {
                if (e instanceof SQLConstraint && e instanceof SQLIndex) {
                    SQLConstraint sqlConstraint = (SQLConstraint) e;
                    if (sqlConstraint.getName() != null) {
                        result.add(SQLUtils.normalize(sqlConstraint.getName().getSimpleName()));
                    }
                }
                if (e instanceof SQLColumnDefinition) {
                    SQLColumnDefinition columnDefinition = (SQLColumnDefinition) e;
                    List<SQLColumnConstraint> constraints = columnDefinition.getConstraints();
                    if (constraints != null) {
                        for (SQLColumnConstraint constraint : constraints) {
                            if (constraint instanceof SQLColumnUniqueKey) {
                                result.add(SQLUtils.normalize(columnDefinition.getName().getSimpleName()));
                            }
                        }
                    }
                }
                if (e instanceof SQLPrimaryKey && includePrimary) {
                    result.add("primary");
                }

            });
        }

        Collection<SchemaObject> objects = schemaRep.getIndexes();
        if (objects != null) {
            objects.forEach(o -> {
                if (o.getStatement() instanceof SQLCreateIndexStatement) {
                    SQLCreateIndexStatement createIndexStatement = (SQLCreateIndexStatement) o.getStatement();
                    String indexTable = SQLUtils.normalize(createIndexStatement.getTableName());
                    if (StringUtils.equalsIgnoreCase(indexTable, table)) {
                        SQLName sqlName = createIndexStatement.getIndexDefinition().getName();
                        if (sqlName != null) {
                            result.add(SQLUtils.normalize(sqlName.getSimpleName()));
                        }
                    }
                }
            });
        }

        return result;
    }

    private SchemaObject findSequence(SchemaRepository repository, String sequenceName) {
        Schema schema = repository.findSchema("d`b1");
        return schema.getStore().getSequence(FnvHash.hashCode64(sequenceName));
    }

    @Test
    public void testSwapColumnName1() {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("t1");
        String createTableSql = "create table t1 (c_dmrs bigint, a bigint primary key, c int, d int, e int)";
        repository.console(createTableSql);

        String alterTableSql =
            "alter table t1 change column c c_dmrs int(11) default null, change column c_dmrs c bigint";
        repository.console(alterTableSql);

        SchemaObject schemaObject = repository.findTable("t1");
        SQLCreateTableStatement stmt1 = (SQLCreateTableStatement) schemaObject.getStatement();
        String newCreateTableSql = "CREATE TABLE t1 (\n"
            + "\tc bigint,\n"
            + "\ta bigint PRIMARY KEY,\n"
            + "\tc_dmrs int(11) DEFAULT NULL,\n"
            + "\td int,\n"
            + "\te int\n"
            + ")";
        Assert.assertEquals(newCreateTableSql, stmt1.toString());
    }

    @Test
    public void testSwapColumnName2() {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("t1");
        String createTableSql = "create table t1 (c_dmrs int, a bigint primary key, c bigint, d int, e int)";
        repository.console(createTableSql);

        String alterTableSql =
            "alter table t1 change column c_dmrs c int(11) default null, change column c c_dmrs bigint";
        repository.console(alterTableSql);

        SchemaObject schemaObject = repository.findTable("t1");
        SQLCreateTableStatement stmt1 = (SQLCreateTableStatement) schemaObject.getStatement();
        String newCreateTableSql = "CREATE TABLE t1 (\n"
            + "\tc int(11) DEFAULT NULL,\n"
            + "\ta bigint PRIMARY KEY,\n"
            + "\tc_dmrs bigint,\n"
            + "\td int,\n"
            + "\te int\n"
            + ")";
        Assert.assertEquals(newCreateTableSql, stmt1.toString());
    }

    @Test
    public void testSwapColumnName3() {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("t1");
        String createTableSql = "create table t1 (c_dmrs bigint, a bigint primary key, c int, d int, e int)";
        repository.console(createTableSql);

        String alterTableSql =
            "alter table t1 change column c c_dmrs int(11) default null, change column c_dmrs c bigint, algorithm=inplace";
        repository.console(alterTableSql);

        SchemaObject schemaObject = repository.findTable("t1");
        SQLCreateTableStatement stmt1 = (SQLCreateTableStatement) schemaObject.getStatement();
        String newCreateTableSql = "CREATE TABLE t1 (\n"
            + "\tc bigint,\n"
            + "\ta bigint PRIMARY KEY,\n"
            + "\tc_dmrs int(11) DEFAULT NULL,\n"
            + "\td int,\n"
            + "\te int\n"
            + ")";
        Assert.assertEquals(newCreateTableSql, stmt1.toString());
    }

    @Test
    public void testMaterializedView() {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("t1");
        String createTableSql = "create materialized view mview as select id from xxx";
        repository.console(createTableSql);
        // com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateMaterializedViewStatement
        // com.alibaba.polardbx.druid.sql.ast.statement.SQLDropMaterializedViewStatement
    }

    @Test
    public void testModifyColumnSequence() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.console(
            "create table `test_compat_yha2_7ajh_00002` (\n"
                + "  a int,\n"
                + "  b double,\n"
                + "  c varchar(10),\n"
                + "  d bigint,\n"
                + "  _drds_implicit_id_ bigint auto_increment,\n"
                + "  primary key (_drds_implicit_id_)\n"
                + ")");
        memoryTableMeta.console("alter table `test_compat_yha2_7ajh_00002`\n"
            + "  modify column a int after b,\n"
            + "  drop column b,\n"
            + "  change column c b int,\n"
            + "  add column c int");

        String expectedDDL = "CREATE TABLE `test_compat_yha2_7ajh_00002` (\n"
            + "\tb int,\n"
            + "\ta int,\n"
            + "\td bigint,\n"
            + "\t_drds_implicit_id_ bigint AUTO_INCREMENT,\n"
            + "\tPRIMARY KEY (_drds_implicit_id_),\n"
            + "\tc int\n"
            + ")";
        SchemaObject tm1 = memoryTableMeta.findTable("test_compat_yha2_7ajh_00002");
        Assert.assertEquals(expectedDDL, tm1.getStatement().toString());
    }

    @Test
    public void testModifyColumnSequence2() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.console(
            "create table `t25` (\n"
                + "  a int,\n"
                + "  b double,\n"
                + "  c varchar(10),\n"
                + "  _drds_implicit_id_ bigint auto_increment,\n"
                + "  d bigint,\n"
                + "  primary key (_drds_implicit_id_)\n"
                + ")");
        String expectedDDL = "CREATE TABLE `t25` (\n"
            + "\tc varchar(10),\n"
            + "\tb double,\n"
            + "\ta int,\n"
            + "\t_drds_implicit_id_ bigint AUTO_INCREMENT,\n"
            + "\td bigint,\n"
            + "\tPRIMARY KEY (_drds_implicit_id_)\n"
            + ")";
        memoryTableMeta.console("alter table t25 modify column a int after c,modify column c varchar(10) first");
        SchemaObject tm1 = memoryTableMeta.findTable("t25");
        Assert.assertEquals(expectedDDL, tm1.getStatement().toString());
    }

    @Test
    public void testAlterRenameAnonymousIndex() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        String createSql = "create table ttt(a int, b int ,c int)";
        String alterAddKey = "alter table ttt add key(a,b,c)";
        String alterRenameKey = "alter table ttt rename key a to a1";
        memoryTableMeta.console(
            createSql);
        memoryTableMeta.console(
            alterAddKey);
        memoryTableMeta.console(
            alterRenameKey);
        SchemaObject tm1 = memoryTableMeta.findTable("ttt");
        System.out.println(tm1.getStatement());
//        Assert.assertEquals(expectedDDL, tm1.getStatement());
    }

    @Test
    public void testAlterColumnOrTableCollate() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        String createSql =
            "create table ttt(a varchar(20), b varchar(20) ,c varchar(20)) default CHARACTER SET = utf8mb4 COLLATE = UTF8MB4_GENERAL_CI";

        memoryTableMeta.console(
            createSql);

        SchemaObject tableTTT = null;
        String alterACollate = "alter table ttt modify column a varchar(20) collate LATIN1_GENERAL_CI";
        memoryTableMeta.console(
            alterACollate);
        tableTTT = memoryTableMeta.findTable("ttt");
        Assert.assertEquals("CREATE TABLE ttt (\n"
                + "\ta varchar(20) CHARACTER SET LATIN1 COLLATE LATIN1_GENERAL_CI,\n"
                + "\tb varchar(20),\n"
                + "\tc varchar(20)\n"
                + ") DEFAULT CHARACTER SET = utf8mb4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
            tableTTT.getStatement().toString());

        String alterTableCollate = "alter table ttt default collate GBK_CHINESE_CI ";
        memoryTableMeta.console(
            alterTableCollate);
        tableTTT = memoryTableMeta.findTable("ttt");
        Assert.assertEquals("CREATE TABLE ttt (\n"
            + "\ta varchar(20) CHARACTER SET LATIN1 COLLATE LATIN1_GENERAL_CI,\n"
            + "\tb varchar(20) CHARACTER SET utf8mb4 COLLATE UTF8MB4_GENERAL_CI,\n"
            + "\tc varchar(20) CHARACTER SET utf8mb4 COLLATE UTF8MB4_GENERAL_CI\n"
            + ") DEFAULT CHARSET = GBK DEFAULT COLLATE = GBK_CHINESE_CI", tableTTT.getStatement().toString());

        String alterBCollate =
            "alter table ttt change column b  b1 varchar(20) CHARACTER SET  LATIN1 collate LATIN1_GENERAL_CI";
        memoryTableMeta.console(
            alterBCollate);
        tableTTT = memoryTableMeta.findTable("ttt");
        Assert.assertEquals("CREATE TABLE ttt (\n"
            + "\ta varchar(20) CHARACTER SET LATIN1 COLLATE LATIN1_GENERAL_CI,\n"
            + "\tb1 varchar(20) CHARACTER SET LATIN1 COLLATE LATIN1_GENERAL_CI,\n"
            + "\tc varchar(20) CHARACTER SET utf8mb4 COLLATE UTF8MB4_GENERAL_CI\n"
            + ") DEFAULT CHARSET = GBK DEFAULT COLLATE = GBK_CHINESE_CI", tableTTT.getStatement().toString());

        String alterTableCharset = "alter table ttt collate = UTF8_GENERAL_CI ";
        memoryTableMeta.console(
            alterTableCharset);
        tableTTT = memoryTableMeta.findTable("ttt");
        Assert.assertEquals("CREATE TABLE ttt (\n"
            + "\ta varchar(20) CHARACTER SET LATIN1 COLLATE LATIN1_GENERAL_CI,\n"
            + "\tb1 varchar(20) CHARACTER SET LATIN1 COLLATE LATIN1_GENERAL_CI,\n"
            + "\tc varchar(20) CHARACTER SET utf8mb4 COLLATE UTF8MB4_GENERAL_CI\n"
            + ") DEFAULT CHARSET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI", tableTTT.getStatement().toString());

        String alterTableCollate1 = "alter table ttt default character set gbk collate GBK_CHINESE_CI ";
        memoryTableMeta.console(
            alterTableCollate1);
        tableTTT = memoryTableMeta.findTable("ttt");
        Assert.assertEquals("CREATE TABLE ttt (\n"
            + "\ta varchar(20) CHARACTER SET LATIN1 COLLATE LATIN1_GENERAL_CI,\n"
            + "\tb1 varchar(20) CHARACTER SET LATIN1 COLLATE LATIN1_GENERAL_CI,\n"
            + "\tc varchar(20) CHARACTER SET utf8mb4 COLLATE UTF8MB4_GENERAL_CI\n"
            + ") DEFAULT CHARACTER SET = gbk COLLATE GBK_CHINESE_CI", tableTTT.getStatement().toString());

    }

    private String findCharset(SQLCreateTableStatement cs) {
        List<SQLAssignItem> assignItems = cs.getTableOptions();
        for (SQLAssignItem ai : assignItems) {
            String key = StringUtils.lowerCase(ai.getTarget().toString());
            if (StringUtils.contains(key, "character") || StringUtils.contains(key, "charset")) {
                return ai.getValue().toString();
            }
        }
        return null;
    }

    @Test
    public void testExtendCharsetFromSchema() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);

        String createDatabase = "create database if not exists `db_test` character set `gbk`";

        memoryTableMeta.console(createDatabase);

        memoryTableMeta.setDefaultSchema("db_test");

        String createSql =
            "create table ttt(a varchar(20), b varchar(20) ,c varchar(20))";

        memoryTableMeta.console(
            createSql);

        SchemaObject schemaObject = memoryTableMeta.findTable("ttt");
        SQLCreateTableStatement cs = (SQLCreateTableStatement) schemaObject.getStatement();
        Assert.assertEquals("gbk", findCharset(cs));

        String alterCharset = "alter table ttt default charset utf8mb4";
        memoryTableMeta.console(alterCharset);
        schemaObject = memoryTableMeta.findTable("ttt");
        cs = (SQLCreateTableStatement) schemaObject.getStatement();
        Assert.assertEquals("utf8mb4", findCharset(cs));

    }

    @Test
    public void testExtendCharsetFromSchemaWithDefaultCharset() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);

        memoryTableMeta.setDefaultCharset("gbk");
        String createDatabase = "create database if not exists `db_test`";

        memoryTableMeta.console(createDatabase);

        memoryTableMeta.setDefaultSchema("db_test");

        String createSql =
            "create table ttt(a varchar(20), b varchar(20) ,c varchar(20))";

        memoryTableMeta.console(
            createSql);

        SchemaObject schemaObject = memoryTableMeta.findTable("ttt");
        SQLCreateTableStatement cs = (SQLCreateTableStatement) schemaObject.getStatement();
        Assert.assertEquals("gbk", findCharset(cs));

        String alterCharset = "alter table ttt default charset utf8mb4";
        memoryTableMeta.console(alterCharset);
        schemaObject = memoryTableMeta.findTable("ttt");
        cs = (SQLCreateTableStatement) schemaObject.getStatement();
        Assert.assertEquals("utf8mb4", findCharset(cs));

    }

    @Test
    public void testCreateTableWithCollation() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);

        memoryTableMeta.setDefaultCharset("gbk");
        String ct =
            "create table if not exists `gxw_test_100_udwv` (\n"
                + "\t`id` int(10) unsigned not null auto_increment,\n"
                + "\t`author_id` int(10) not null default 0,\n"
                + "\t`platform` varchar(255) collate utf8_unicode_ci null default null,\n"
                + "\tprimary key (`id`),\n"
                + "\tindex `idx_author_id_platform`(`author_id`, `platform`)\n"
                + ") default collate = 'utf8_unicode_ci' engine = 'innodb'";
        memoryTableMeta.console(ct);
        SchemaObject so = memoryTableMeta.findTable("gxw_test_100_udwv");
        MySqlCreateTableStatement cs = (MySqlCreateTableStatement) so.getStatement();

        String expect = "CREATE TABLE IF NOT EXISTS `gxw_test_100_udwv` (\n"
            + "\t`id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
            + "\t`author_id` int(10) NOT NULL DEFAULT 0,\n"
            + "\t`platform` varchar(255) COLLATE utf8_unicode_ci NULL DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tINDEX `idx_author_id_platform`(`author_id`, `platform`)\n"
            + ") DEFAULT CHARACTER SET = UTF8 ENGINE = 'innodb' DEFAULT COLLATE = 'utf8_unicode_ci'";
        Assert.assertEquals(expect, cs.toString());
    }

    @Test
    public void testCreateTableWithCollationDisordered() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);

        memoryTableMeta.setDefaultCharset("gbk");
        String ct =
            "create table if not exists `collation_disorder` (\n"
                + "\t`id` int(10) unsigned not null auto_increment,\n"
                + "\t`author_id` int(10) not null default 0,\n"
                + "\t`platform` varchar(255) collate utf8_unicode_ci null default null,\n"
                + "\tprimary key (`id`),\n"
                + "\tindex `idx_author_id_platform`(`author_id`, `platform`)\n"
                + ") default collate = 'utf8_unicode_ci' DEFAULT CHARACTER SET = UTF8 engine = 'innodb'";
        memoryTableMeta.console(ct);
        SchemaObject so = memoryTableMeta.findTable("collation_disorder");
        MySqlCreateTableStatement cs = (MySqlCreateTableStatement) so.getStatement();

        String expect = "CREATE TABLE IF NOT EXISTS `collation_disorder` (\n"
            + "\t`id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
            + "\t`author_id` int(10) NOT NULL DEFAULT 0,\n"
            + "\t`platform` varchar(255) COLLATE utf8_unicode_ci NULL DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tINDEX `idx_author_id_platform`(`author_id`, `platform`)\n"
            + ") DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = 'utf8_unicode_ci' ENGINE = 'innodb'";
        Assert.assertEquals(expect, cs.toString());
    }

    @Test
    public void testCreateFunction() {
        // see https://aone.alibaba-inc.com/v2/project/860366/bug/55231933
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "create function fac(n int unsigned) returns bigint unsigned\n"
            + "begin\n"
            + "declare f bigint unsigned default 1;\n"
            + "\n"
            + "while n > 1 do\n"
            + "set f = f * n;\n"
            + "set n = n - 1;\n"
            + "end while;\n"
            + "return f;\n"
            + "end";
        memoryTableMeta.console(sql);

        String sql2 = "CREATE FUNCTION my_func (input_param INT) RETURNS INT\n"
            + "BEGIN\n"
            + "  RETURN input_param / 2;\n"
            + "END";
        memoryTableMeta.console(sql2);

    }

    @Test
    public void testDropFunction() {
        // see https://aone.alibaba-inc.com/v2/project/860366/bug/55231933
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "drop function if exists fac";
        memoryTableMeta.console(sql);
    }

    @Test
    public void testCreateProcedure() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "CREATE PROCEDURE ComplexProcedure(\n"
            + "    IN in_param1 INT,\n"
            + "    IN in_param2 VARCHAR(255),\n"
            + "    OUT out_param INT\n"
            + ")\n"
            + "BEGIN\n"
            + "    DECLARE var1 INT DEFAULT 0;\n"
            + "    DECLARE var2 INT DEFAULT 0;\n"
            + "\n"
            + "    -- 开始一个新的事务\n"
            + "    START TRANSACTION;\n"
            + "\n"
            + "    -- 插入数据到表A\n"
            + "    INSERT INTO tableA (column1, column2) VALUES (in_param1, in_param2);\n"
            + "\n"
            + "    -- 执行一些计算或逻辑判断\n"
            + "    SELECT COUNT(*) INTO var1 FROM tableB WHERE condition_column = in_param1;\n"
            + "    \n"
            + "    IF var1 > 0 THEN\n"
            + "        SET var2 = 1;\n"
            + "        UPDATE tableC SET status = 'active' WHERE id = in_param1;\n"
            + "    ELSE\n"
            + "        SET var2 = 0;\n"
            + "        INSERT INTO tableD (ref_id, message) VALUES (in_param1, 'No matching records found');\n"
            + "    END IF;\n"
            + "\n"
            + "    -- 将计算结果作为输出参数\n"
            + "    SET out_param = var2;\n"
            + "\n"
            + "    -- 如果所有操作成功，则提交事务\n"
            + "    COMMIT;\n"
            + "\n"
            + "END";
        memoryTableMeta.console(sql);
    }

    @Test
    public void testDropProcedure() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "drop PROCEDURE if exists SimpleProcedure";
        memoryTableMeta.console(sql);
    }

    @Test
    public void testSavePoint() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "SAVEPOINT sp1";
        memoryTableMeta.console(sql);
    }

    @Test
    public void testAlterUser() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "ALTER USER 'jeffrey'@'localhost'\n"
            + "  IDENTIFIED BY 'new_password' PASSWORD EXPIRE;";
        memoryTableMeta.console(sql);

        String sql4 = "ALTER USER 'jeffrey'@'localhost'\n"
            + "  IDENTIFIED WITH 'auth_plugin' BY 'new_password' PASSWORD EXPIRE;";
        memoryTableMeta.console(sql4);

        String sql5 = "ALTER USER 'jeffrey'@'localhost'\n"
            + "  IDENTIFIED WITH 'auth_plugin' AS 'new_password' PASSWORD EXPIRE;";
        memoryTableMeta.console(sql5);

        String sql1 = "ALTER USER 'suspended_user'@'localhost' ACCOUNT LOCK;";
        memoryTableMeta.console(sql1);

        String sql2 = "ALTER USER 'suspended_user'@'localhost' ACCOUNT UNLOCK";
        memoryTableMeta.console(sql2);

        String sql3 = "ALTER USER 'jeffrey'@'localhost'\n"
            + "  REQUIRE SSL WITH MAX_CONNECTIONS_PER_HOUR 20;";
        memoryTableMeta.console(sql3);

        String sql6 = "ALTER USER 'admin_user'@'localhost' WITH GRANT OPTION;";
        memoryTableMeta.console(sql6);
    }

    @Test
    public void testGrant() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        String sql1 = "GRANT ALL PRIVILEGES ON *.* TO 'powerful_user'@'localhost' IDENTIFIED BY 'strong_password';";
        memoryTableMeta.console(sql1);

        String sql2 = "GRANT SELECT, INSERT, UPDATE ON `database_name`.* TO 'readonly_write'@'%';";
        memoryTableMeta.console(sql2);

        String sql3 =
            "GRANT SELECT, DELETE ON `database_name`.`specific_table` TO 'table_specific_user'@'192.168.1.0/24';";
        memoryTableMeta.console(sql3);

        String sql4 =
            "GRANT CREATE TEMPORARY TABLES ON *.* TO 'temp_table_creator'@'localhost';";
        memoryTableMeta.console(sql4);

        String sql5 =
            "GRANT SELECT ON *.* TO 'backup_user'@'backup_server_ip' IDENTIFIED BY 'backup_password';";
        memoryTableMeta.console(sql5);
    }

    @Test
    public void testCreateFunction2() {
        // see https://aone.alibaba-inc.com/v2/project/860366/bug/55916958
        String sql1 = "create table t3(id int)";
        String sql2 = "create function mysql.f9 () returns int contains sql sql security definer begin "
            + "declare a, b int; "
            + "drop temporary table if exists t3; "
            + "create temporary table t3 ( id int ); "
            + "insert into t3 values (1), (2), (3); "
            + "set a = ( select count(*) from t3 ); "
            + "set b = ( select count(*) from t3 t3_alias ); "
            + "return a + b; "
            + "end";
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.setDefaultSchema("d1");
        memoryTableMeta.console(sql1);
        memoryTableMeta.console(sql2);
        Schema schema = memoryTableMeta.findSchema("d1");
        SchemaObject schemaObject = schema.findTable("t3");
        Assert.assertNotNull(schemaObject);
    }

    @Test
    public void testAlterMulCol() {

        String expect = "CREATE TABLE test_compat_2wzg (\n"
            + "\tb int,\n"
            + "\ta int,\n"
            + "\td bigint,\n"
            + "\t_drds_implicit_id_ bigint AUTO_INCREMENT,\n"
            + "\tPRIMARY KEY (_drds_implicit_id_),\n"
            + "\tc int\n"
            + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`";

        String sql1 = "CREATE TABLE test_compat_2wzg (\n"
            + "  a int,\n"
            + "  b double,\n"
            + "  c varchar(10),\n"
            + "  d bigint,\n"
            + "  _drds_implicit_id_ bigint AUTO_INCREMENT,\n"
            + "  PRIMARY KEY (_drds_implicit_id_)\n"
            + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`";
        String sql2 = "alter table test_compat_2wzg\n"
            + "  MODIFY COLUMN a int AFTER b,\n"
            + "  CHANGE COLUMN c b int,\n"
            + "  DROP COLUMN b,\n"
            + "  ADD COLUMN c int";
        SchemaRepository memoryTableMeta1 = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta1.setDefaultSchema("d1");
        memoryTableMeta1.console(sql1);
        memoryTableMeta1.console(sql2);
        SchemaObject tableMeta1 = memoryTableMeta1.findSchema("d1").findTable("test_compat_2wzg");
        Assert.assertEquals(expect, tableMeta1.getStatement().toString());

    }

    @Test
    public void testTableWithBlankName() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.setDefaultSchema("d1");

        String sql1 = "CREATE TABLE ` engineer` (\n"
            + "  id INTEGER NOT NULL,\n"
            + "  engineer_name VARCHAR(50),\n"
            + "  PRIMARY KEY (id)\n"
            + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`";
        String sql2 = "CREATE TABLE `engineer` (\n"
            + "  id INTEGER NOT NULL,\n"
            + "  engineer_name VARCHAR(50),\n"
            + "  engineer_type VARCHAR(50),\n"
            + "  PRIMARY KEY (id)\n"
            + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`";
        memoryTableMeta.console(sql1);
        memoryTableMeta.console(sql2);
        SchemaObject tableMeta1 = memoryTableMeta.findSchema("d1").findTable(" engineer");
        SchemaObject tableMeta2 = memoryTableMeta.findSchema("d1").findTable("engineer");
        System.out.println(tableMeta1.getStatement().toString());
        System.out.println(tableMeta2.getStatement().toString());
        Assert.assertEquals("CREATE TABLE ` engineer` (\n"
                + "\tid INTEGER NOT NULL,\n"
                + "\tengineer_name VARCHAR(50),\n"
                + "\tPRIMARY KEY (id)\n"
                + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`",
            tableMeta1.getStatement().toString());
        Assert.assertEquals("CREATE TABLE `engineer` (\n"
                + "\tid INTEGER NOT NULL,\n"
                + "\tengineer_name VARCHAR(50),\n"
                + "\tengineer_type VARCHAR(50),\n"
                + "\tPRIMARY KEY (id)\n"
                + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`",
            tableMeta2.getStatement().toString());

        String sql3 = "alter table ` engineer` add column engineer_age bigint";
        String sql4 = "alter table `engineer` add column engineer_age bigint";
        memoryTableMeta.console(sql3);
        memoryTableMeta.console(sql4);
        SchemaObject tableMeta3 = memoryTableMeta.findSchema("d1").findTable(" engineer");
        SchemaObject tableMeta4 = memoryTableMeta.findSchema("d1").findTable("engineer");
        System.out.println(tableMeta3.getStatement().toString());
        System.out.println(tableMeta4.getStatement().toString());
        Assert.assertEquals("CREATE TABLE ` engineer` (\n"
                + "\tid INTEGER NOT NULL,\n"
                + "\tengineer_name VARCHAR(50),\n"
                + "\tPRIMARY KEY (id),\n"
                + "\tengineer_age bigint\n"
                + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`",
            tableMeta3.getStatement().toString());
        Assert.assertEquals("CREATE TABLE `engineer` (\n"
                + "\tid INTEGER NOT NULL,\n"
                + "\tengineer_name VARCHAR(50),\n"
                + "\tengineer_type VARCHAR(50),\n"
                + "\tPRIMARY KEY (id),\n"
                + "\tengineer_age bigint\n"
                + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`",
            tableMeta4.getStatement().toString());

        String sql5 = "rename table ` engineer` to ` engineer_new`";
        String sql6 = "rename table `engineer` to `engineer_new`";
        memoryTableMeta.console(sql5);
        memoryTableMeta.console(sql6);
        SchemaObject tableMeta5 = memoryTableMeta.findSchema("d1").findTable(" engineer_new");
        SchemaObject tableMeta6 = memoryTableMeta.findSchema("d1").findTable("engineer_new");
        System.out.println(tableMeta5.getStatement().toString());
        System.out.println(tableMeta6.getStatement().toString());
        Assert.assertEquals("CREATE TABLE ` engineer_new` (\n"
                + "\tid INTEGER NOT NULL,\n"
                + "\tengineer_name VARCHAR(50),\n"
                + "\tPRIMARY KEY (id),\n"
                + "\tengineer_age bigint\n"
                + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`",
            tableMeta5.getStatement().toString());
        Assert.assertEquals("CREATE TABLE `engineer_new` (\n"
                + "\tid INTEGER NOT NULL,\n"
                + "\tengineer_name VARCHAR(50),\n"
                + "\tengineer_type VARCHAR(50),\n"
                + "\tPRIMARY KEY (id),\n"
                + "\tengineer_age bigint\n"
                + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`",
            tableMeta6.getStatement().toString());

        String sql7 = "drop table ` engineer_new`";
        String sql8 = "drop table `engineer_new`";
        memoryTableMeta.console(sql7);
        memoryTableMeta.console(sql8);
        SchemaObject tableMeta7 = memoryTableMeta.findSchema("d1").findTable(" engineer_new");
        SchemaObject tableMeta8 = memoryTableMeta.findSchema("d1").findTable("engineer_new");
        System.out.println(tableMeta7);
        System.out.println(tableMeta8);
    }

    @Test
    public void testTableWithLocalPartition() {

        String sql1 = "ALTER TABLE t_ttl_single\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '2023-08-20'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 1\n"
            + "PRE ALLOCATE 3\n"
            + "PIVOTDATE now()";
        String sql2 = "ALTER TABLE t_ttl_single\n"
            + "\t LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "\t STARTWITH '2023-08-20'\n"
            + "\t INTERVAL 1 MONTH\n"
            + "\t EXPIRE AFTER 1\n"
            + "\t PRE ALLOCATE 3\n"
            + "\t PIVOTDATE now()";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql1, DbType.mysql, FEATURES);
        StringBuffer sb = new StringBuffer();
        stmtList.get(0).output(sb);
        Assert.assertEquals(sql2, sb.toString());
    }

    @Test
    public void testCreateTableWithLocalPartition() {

        String sql1 = "CREATE TABLE t_local_partition (\n"
            + "    id bigint NOT NULL AUTO_INCREMENT,\n"
            + "    gmt_modified DATETIME NOT NULL,\n"
            + "    PRIMARY KEY (id, gmt_modified)\n"
            + ")\n"
            + "PARTITION BY HASH(id) PARTITIONS 8\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '2023-08-20'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 1\n"
            + "PRE ALLOCATE 3\n"
            + "PIVOTDATE NOW();";
        String sql2 = "CREATE TABLE t_local_partition (\n"
            + "\tid bigint NOT NULL AUTO_INCREMENT,\n"
            + "\tgmt_modified DATETIME NOT NULL,\n"
            + "\tPRIMARY KEY (id, gmt_modified)\n"
            + ")\n"
            + "PARTITION BY HASH (id) PARTITIONS 8\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + " STARTWITH '2023-08-20'\n"
            + " INTERVAL 1 MONTH\n"
            + " EXPIRE AFTER 1\n"
            + " PRE ALLOCATE 3\n"
            + " PIVOTDATE NOW();";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql1, DbType.mysql, FEATURES);
        StringBuffer sb = new StringBuffer();
        stmtList.get(0).output(sb);
        System.out.println(sb);
        Assert.assertEquals(sql2, sb.toString());
    }

    @Test
    public void testUdfHash() {

        String sql = "CREATE TABLE `account_device_mapping` (\n"
            + "        `gameUid` varchar(100) COLLATE utf8_unicode_ci NOT NULL,\n"
            + "        `deviceId` varchar(200) COLLATE utf8_unicode_ci NOT NULL,\n"
            + "        `type` int(11) DEFAULT NULL,\n"
            + "        `time` bigint(20) DEFAULT NULL,\n"
            + "        PRIMARY KEY (`gameUid`, `deviceId`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_unicode_ci\n"
            + "PARTITION BY UDF_HASH (`gameUid` WITH UDF_PARAMS ( 'dble/hash','{\"partitionCount\" : \"32\",\"partitionLength\" : \"32\"}')) PARTITIONS 32";
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.setDefaultSchema("d1");
        memoryTableMeta.console(sql);
        SchemaObject tableMeta7 = memoryTableMeta.findSchema("d1").findTable("account_device_mapping");
        System.out.println(tableMeta7.getStatement().toString());
        String formatSql = tableMeta7.getStatement().toString();
        List<SQLStatement> stmtList = SQLUtils.parseStatements(formatSql, DbType.mysql, FEATURES);
        List<SQLStatement> stmtList2 = SQLUtils.parseStatements(sql, DbType.mysql, FEATURES);
        Assert.assertEquals(stmtList2.size(), stmtList.size());
        for (int i = 0; i < stmtList.size(); i++) {
            SQLStatement stmt1 = stmtList.get(i);
            SQLStatement stmt2 = stmtList2.get(i);
            Assert.assertEquals(stmt1.toString(), stmt2.toString());
        }
    }

    @Test
    public void testConvertGelCol() {

        String sql = "CREATE TABLE `account_device_mapping` (\n"
            + "        `gameUid` varchar(100) COLLATE utf8_unicode_ci NOT NULL,\n"
            + "        `deviceId` varchar(200) COLLATE utf8_unicode_ci NOT NULL,\n"
            + "        `type` int(11) DEFAULT NULL,\n"
            + "        `time` bigint(20) DEFAULT NULL,\n"
            + "        `gen_ycv1gp3dh` varchar(64) GENERATED ALWAYS AS (CAST(`c_idx` AS char)) VIRTUAL,\n"
            + "        PRIMARY KEY (`gameUid`, `deviceId`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_unicode_ci\n"
            + "PARTITION BY UDF_HASH (`gameUid` WITH UDF_PARAMS ( 'dble/hash','{\"partitionCount\" : \"32\",\"partitionLength\" : \"32\"}')) PARTITIONS 32";
        String sql2 = "alter table `account_device_mapping`\n"
            + "\tconvert to character set utf8";
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.setDefaultSchema("d1");
        memoryTableMeta.console(sql);
        memoryTableMeta.console(sql2);
        SchemaObject tableMeta7 = memoryTableMeta.findSchema("d1").findTable("account_device_mapping");
        System.out.println(tableMeta7.getStatement().toString());
        String formatSql = tableMeta7.getStatement().toString();
        List<SQLStatement> stmtList = SQLUtils.parseStatements(formatSql, DbType.mysql, FEATURES);
        List<SQLStatement> stmtList2 = SQLUtils.parseStatements(sql, DbType.mysql, FEATURES);
        Assert.assertEquals(stmtList2.size(), stmtList.size());
        for (int i = 0; i < stmtList.size(); i++) {
            SQLStatement stmt1 = stmtList.get(i);
            SQLStatement stmt2 = stmtList2.get(i);
            Assert.assertEquals(stmt1.toString(), stmt2.toString());
        }
    }

    /**
     * Comprehensive test for CONVERT TO CHARACTER SET with all PolarDB-X 2.0 column types.
     * Verifies:
     * 1. Charset-sensitive types (char/varchar/text family/enum/set) are correctly converted
     * 2. NCHAR/NVARCHAR are converted to CHAR/VARCHAR
     * 3. TEXT types upgrade when converting to wider charset (tinytext->text->mediumtext->longtext)
     * 4. Non-charset types (int/binary/blob/json/date/decimal etc.) are NOT touched
     * 5. GENERATED columns are completely skipped (column inherits charset from table DEFAULT CHARSET;
     * CAST expressions inside use character_set_connection, not table charset)
     * 6. Same-charset conversion is a no-op
     */
    @Test
    public void testConvertToCharacterSetAllColumnTypes() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.setDefaultSchema("d1");

        // Create table with all PolarDB-X 2.0 column types, using latin1 as base charset
        String createSql = "CREATE TABLE `all_col_types` (\n"
            + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            // -- charset-sensitive types --
            + "  `c_char` char(50) DEFAULT 'abc',\n"
            + "  `c_char_df` char DEFAULT 'x',\n"
            + "  `c_varchar` varchar(200) DEFAULT 'hello',\n"
            + "  `c_tinytext` tinytext DEFAULT NULL,\n"
            + "  `c_text` text DEFAULT NULL,\n"
            + "  `c_mediumtext` mediumtext DEFAULT NULL,\n"
            + "  `c_longtext` longtext DEFAULT NULL,\n"
            + "  `c_enum` enum('a','b','c') DEFAULT 'a',\n"
            + "  `c_set` set('x','y','z') DEFAULT 'x',\n"
            // -- charset-sensitive with explicit charset/collate --
            + "  `c_varchar_charset` varchar(100) CHARACTER SET gbk DEFAULT NULL,\n"
            + "  `c_varchar_collate` varchar(100) COLLATE latin1_german1_ci DEFAULT NULL,\n"
            // -- national char (converted to char/varchar on CONVERT) --
            + "  `c_nchar` nchar(100) DEFAULT NULL,\n"
            + "  `c_nvarchar` nvarchar(100) DEFAULT NULL,\n"
            // -- generated column (should be converted, consistent with MySQL) --
            + "  `c_gen_virtual` varchar(64) GENERATED ALWAYS AS (CAST(`id` AS char)) VIRTUAL,\n"
            // -- non-charset-sensitive: binary types --
            + "  `c_binary` binary(50) DEFAULT NULL,\n"
            + "  `c_varbinary` varbinary(200) DEFAULT NULL,\n"
            + "  `c_tinyblob` tinyblob DEFAULT NULL,\n"
            + "  `c_blob` blob DEFAULT NULL,\n"
            + "  `c_mediumblob` mediumblob DEFAULT NULL,\n"
            + "  `c_longblob` longblob DEFAULT NULL,\n"
            // -- non-charset-sensitive: numeric types --
            + "  `c_tinyint` tinyint(4) DEFAULT 0,\n"
            + "  `c_smallint` smallint(6) DEFAULT 0,\n"
            + "  `c_mediumint` mediumint(9) DEFAULT 0,\n"
            + "  `c_int` int(11) DEFAULT 0,\n"
            + "  `c_bigint` bigint(20) DEFAULT 0,\n"
            + "  `c_decimal` decimal(10,2) DEFAULT 0,\n"
            + "  `c_float` float DEFAULT 0,\n"
            + "  `c_double` double DEFAULT 0,\n"
            + "  `c_bit` bit(8) DEFAULT NULL,\n"
            // -- non-charset-sensitive: date/time types --
            + "  `c_date` date DEFAULT NULL,\n"
            + "  `c_datetime` datetime DEFAULT NULL,\n"
            + "  `c_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `c_time` time DEFAULT NULL,\n"
            + "  `c_year` year DEFAULT NULL,\n"
            // -- non-charset-sensitive: json/geo --
            + "  `c_json` json DEFAULT NULL,\n"
            + "  `c_geo` geometry DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = latin1";

        memoryTableMeta.console(createSql, FEATURES);

        // ========== Test 1: CONVERT TO wider charset (latin1 -> utf8mb4) ==========
        String convertSql = "ALTER TABLE `all_col_types` CONVERT TO CHARACTER SET utf8mb4";
        memoryTableMeta.console(convertSql, FEATURES);

        SchemaObject table = memoryTableMeta.findSchema("d1").findTable("all_col_types");
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) table.getStatement();

        // Charset-sensitive columns should have charset set to utf8mb4
        for (String colName : new String[] {
            "c_char", "c_char_df", "c_varchar",
            "c_tinytext", "c_text", "c_mediumtext", "c_longtext",
            "c_enum", "c_set",
            "c_varchar_charset", "c_varchar_collate"}) {
            SQLColumnDefinition col = stmt.findColumn(colName);
            Assert.assertNotNull("Column " + colName + " charsetExpr should not be null", col.getCharsetExpr());
            Assert.assertEquals("Column " + colName + " charset mismatch",
                "utf8mb4", col.getCharsetExpr().toString());
        }

        // NCHAR/NVARCHAR should be converted to CHAR/VARCHAR with utf8mb4 charset
        Assert.assertEquals("CHAR", stmt.findColumn("c_nchar").getDataType().getName());
        Assert.assertEquals("VARCHAR", stmt.findColumn("c_nvarchar").getDataType().getName());
        Assert.assertNotNull(stmt.findColumn("c_nchar").getCharsetExpr());
        Assert.assertEquals("utf8mb4", stmt.findColumn("c_nchar").getCharsetExpr().toString());
        Assert.assertNotNull(stmt.findColumn("c_nvarchar").getCharsetExpr());
        Assert.assertEquals("utf8mb4", stmt.findColumn("c_nvarchar").getCharsetExpr().toString());

        // TEXT type upgrade: latin1 (maxLen=1) -> utf8mb4 (maxLen=4)
        Assert.assertEquals("TEXT", stmt.findColumn("c_tinytext").getDataType().getName());
        Assert.assertEquals("MEDIUMTEXT", stmt.findColumn("c_text").getDataType().getName());
        Assert.assertEquals("LONGTEXT", stmt.findColumn("c_mediumtext").getDataType().getName());
        Assert.assertEquals("longtext", stmt.findColumn("c_longtext").getDataType().getName());

        // GENERATED column should be completely skipped:
        // - No explicit column-level charset (would produce invalid SQL after VIRTUAL/STORED)
        // - CAST charset NOT modified (CAST uses character_set_connection, not table charset)
        // - Column inherits charset from table DEFAULT CHARSET implicitly
        SQLColumnDefinition genCol = stmt.findColumn("c_gen_virtual");
        Assert.assertNotNull(genCol.getGeneratedAlawsAs());
        Assert.assertNull("Generated column should NOT have explicit column-level charset",
            genCol.getCharsetExpr());

        // Non-charset-sensitive columns should NOT have charset set
        for (String colName : new String[] {
            "c_binary", "c_varbinary",
            "c_tinyblob", "c_blob", "c_mediumblob", "c_longblob",
            "c_tinyint", "c_smallint", "c_mediumint", "c_int", "c_bigint",
            "c_decimal", "c_float", "c_double", "c_bit",
            "c_date", "c_datetime", "c_timestamp", "c_time", "c_year",
            "c_json", "c_geo"}) {
            SQLColumnDefinition col = stmt.findColumn(colName);
            Assert.assertNull("Column " + colName + " should NOT have charset", col.getCharsetExpr());
        }

        // Table charset should be updated
        List<SQLAssignItem> opts = stmt.getTableOptions();
        String tableCharset = null;
        for (SQLAssignItem item : opts) {
            if (item.getTarget().toString().contains("CHARSET") ||
                item.getTarget().toString().contains("CHARACTER")) {
                tableCharset = item.getValue().toString();
            }
        }
        Assert.assertEquals("utf8mb4", tableCharset);

        // Verify the output SQL can be re-parsed
        String outputSql = stmt.toString();
        List<SQLStatement> reparsed = SQLUtils.parseStatements(outputSql, DbType.mysql, FEATURES);
        Assert.assertEquals(1, reparsed.size());

        // ========== Test 2: CONVERT TO same charset (utf8mb4 -> utf8mb4) should be no-op ==========
        String convertSameSql = "ALTER TABLE `all_col_types` CONVERT TO CHARACTER SET utf8mb4";
        memoryTableMeta.console(convertSameSql, FEATURES);
        SchemaObject table2 = memoryTableMeta.findSchema("d1").findTable("all_col_types");
        MySqlCreateTableStatement stmt2 = (MySqlCreateTableStatement) table2.getStatement();
        // Output should be identical to previous
        Assert.assertEquals(outputSql, stmt2.toString());

        // ========== Test 3: CONVERT TO with explicit COLLATE ==========
        String convertWithCollate = "ALTER TABLE `all_col_types` CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci";
        memoryTableMeta.console(convertWithCollate, FEATURES);
        SchemaObject table3 = memoryTableMeta.findSchema("d1").findTable("all_col_types");
        MySqlCreateTableStatement stmt3 = (MySqlCreateTableStatement) table3.getStatement();

        // All charset-sensitive columns (excluding generated) should now have utf8 charset and utf8_unicode_ci collate
        for (String colName : new String[] {
            "c_char", "c_char_df", "c_varchar",
            "c_enum", "c_set", "c_nchar", "c_nvarchar",
            "c_varchar_charset", "c_varchar_collate"}) {
            SQLColumnDefinition col = stmt3.findColumn(colName);
            Assert.assertNotNull("Column " + colName + " charsetExpr should not be null after collate convert",
                col.getCharsetExpr());
            Assert.assertEquals("Column " + colName + " charset mismatch",
                "utf8", col.getCharsetExpr().toString());
            Assert.assertNotNull("Column " + colName + " collateExpr should not be null",
                col.getCollateExpr());
            Assert.assertEquals("Column " + colName + " collate mismatch",
                "utf8_unicode_ci", col.getCollateExpr().toString());
        }

        // Non-charset-sensitive columns remain untouched
        for (String colName : new String[] {
            "c_binary", "c_varbinary", "c_blob",
            "c_tinyint", "c_int", "c_bigint", "c_decimal", "c_float", "c_double",
            "c_date", "c_datetime", "c_timestamp", "c_json", "c_geo"}) {
            SQLColumnDefinition col = stmt3.findColumn(colName);
            Assert.assertNull("Column " + colName + " should NOT have charset", col.getCharsetExpr());
            Assert.assertNull("Column " + colName + " should NOT have collate", col.getCollateExpr());
        }

        // GENERATED column should remain untouched (skipped during CONVERT)
        SQLColumnDefinition genCol3 = stmt3.findColumn("c_gen_virtual");
        Assert.assertNull("Generated column should NOT have explicit column-level charset",
            genCol3.getCharsetExpr());
        Assert.assertNull("Generated column should NOT have explicit collate",
            genCol3.getCollateExpr());

        // Verify re-parseable
        String outputSql3 = stmt3.toString();
        List<SQLStatement> reparsed3 = SQLUtils.parseStatements(outputSql3, DbType.mysql, FEATURES);
        Assert.assertEquals(1, reparsed3.size());
    }

    /**
     * Test MySQL behavior for CAST expression charset in GENERATED columns during ALTER TABLE operations.
     * <p>
     * Verifies:
     * 1. CAST expressions with explicit charset in GENERATED columns are NOT modified by CONVERT TO CHARACTER SET
     * 2. Regular columns' charset ARE modified by CONVERT TO CHARACTER SET
     * 3. Adding GENERATED column with CAST expression uses utf8mb4 charset in CAST
     * 4. Adding GENERATED column with explicit column-level charset keeps column charset separate from CAST charset
     * <p>
     * This test mimics the following MySQL behavior:
     * - CREATE TABLE with CAST(raw AS CHAR(50)) STORED -> CAST uses utf8mb4
     * - ALTER TABLE CONVERT TO CHARACTER SET gbk -> regular columns change to gbk, CAST charset stays utf8mb4
     * - ALTER TABLE ADD COLUMN gen AS (CAST(id AS CHAR)) STORED -> CAST uses utf8mb4
     * - ALTER TABLE CONVERT TO CHARACTER SET utf8 -> regular columns change to utf8, CAST charset stays utf8mb4
     * - ALTER TABLE ADD COLUMN gen CHARACTER SET gbk AS (CAST(id AS CHAR)) STORED -> column charset is gbk, CAST charset is utf8mb4
     */
    @Test
    public void testCastExpressionCharsetInGeneratedColumnAlter() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.setDefaultSchema("d1");

        // ========== Step 1: Create table with latin1 charset and CAST expression ==========
        String createSql = "CREATE TABLE `t_cast_explicit_alter_b` (\n"
            + "  `id` int NOT NULL,\n"
            + "  `raw` varchar(100) CHARACTER SET latin1 DEFAULT NULL,\n"
            + "  `gen_cast_explicit` varchar(100) GENERATED ALWAYS AS (CAST(`raw` AS char(50))) STORED,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = latin1";

        memoryTableMeta.console(createSql, FEATURES);
        SchemaObject table1 = memoryTableMeta.findSchema("d1").findTable("t_cast_explicit_alter_b");
        MySqlCreateTableStatement stmt1 = (MySqlCreateTableStatement) table1.getStatement();

        // Verify initial state
        SQLColumnDefinition rawCol1 = stmt1.findColumn("raw");
        // Column has explicit CHARACTER SET latin1, but it might be stored or inherited
        // Just verify the column exists
        Assert.assertNotNull(rawCol1);

        SQLColumnDefinition genCol1 = stmt1.findColumn("gen_cast_explicit");
        Assert.assertNotNull(genCol1.getGeneratedAlawsAs());
        // Generated column should not have explicit column-level charset
        Assert.assertNull("Generated column should NOT have explicit column-level charset initially",
            genCol1.getCharsetExpr());

        // ========== Step 2: CONVERT TO CHARACTER SET gbk ==========
        String convertToGbk = "ALTER TABLE `t_cast_explicit_alter_b` CONVERT TO CHARACTER SET gbk";
        memoryTableMeta.console(convertToGbk, FEATURES);

        SchemaObject table2 = memoryTableMeta.findSchema("d1").findTable("t_cast_explicit_alter_b");
        MySqlCreateTableStatement stmt2 = (MySqlCreateTableStatement) table2.getStatement();

        // Regular column 'raw' should now have gbk charset after CONVERT
        SQLColumnDefinition rawCol2 = stmt2.findColumn("raw");
        Assert.assertNotNull("raw column should have charset after CONVERT", rawCol2.getCharsetExpr());
        Assert.assertEquals("gbk", rawCol2.getCharsetExpr().toString());

        // Generated column WITHOUT explicit charset should still NOT have explicit column-level charset
        // Because it will use the new table charset (gbk) by default
        SQLColumnDefinition genCol2 = stmt2.findColumn("gen_cast_explicit");
        Assert.assertNotNull(genCol2.getGeneratedAlawsAs());
        Assert.assertNull("Generated column without explicit charset should NOT have charsetExpr after CONVERT",
            genCol2.getCharsetExpr());
        // The CAST expression should be present (without CHARACTER SET, as MySQL doesn't output it for CAST)
        String genSql2 = genCol2.getGeneratedAlawsAs().toString();
        Assert.assertTrue("CAST expression should be present",
            genSql2.toLowerCase().contains("cast"));

        // Table charset should be gbk
        String tableCharset2 = getTableCharset(stmt2);
        Assert.assertEquals("gbk", tableCharset2);

        // Verify output SQL can be re-parsed
        String outputSql2 = stmt2.toString();
        List<SQLStatement> reparsed2 = SQLUtils.parseStatements(outputSql2, DbType.mysql, FEATURES);
        Assert.assertEquals(1, reparsed2.size());

        // ========== Step 3: Add new GENERATED column with CAST expression ==========
        String addGenCol = "ALTER TABLE `t_cast_explicit_alter_b` "
            + "ADD COLUMN `gen_lcunpk8x` varchar(64) GENERATED ALWAYS AS (CAST(`id` AS char)) STORED";
        memoryTableMeta.console(addGenCol, FEATURES);

        SchemaObject table3 = memoryTableMeta.findSchema("d1").findTable("t_cast_explicit_alter_b");
        MySqlCreateTableStatement stmt3 = (MySqlCreateTableStatement) table3.getStatement();

        // New generated column should not have explicit column-level charset
        SQLColumnDefinition newGenCol3 = stmt3.findColumn("gen_lcunpk8x");
        Assert.assertNotNull(newGenCol3.getGeneratedAlawsAs());
        Assert.assertNull("New generated column should NOT have explicit column-level charset",
            newGenCol3.getCharsetExpr());

        // Existing columns should remain unchanged
        SQLColumnDefinition rawCol3 = stmt3.findColumn("raw");
        Assert.assertNotNull(rawCol3.getCharsetExpr());
        Assert.assertEquals("gbk", rawCol3.getCharsetExpr().toString());

        SQLColumnDefinition genCol3 = stmt3.findColumn("gen_cast_explicit");
        Assert.assertNull("Original generated column should still NOT have charset",
            genCol3.getCharsetExpr());

        // Verify output SQL can be re-parsed
        String outputSql3 = stmt3.toString();
        List<SQLStatement> reparsed3 = SQLUtils.parseStatements(outputSql3, DbType.mysql, FEATURES);
        Assert.assertEquals(1, reparsed3.size());

        // ========== Step 4: CONVERT TO CHARACTER SET utf8 ==========
        String convertToUtf8 = "ALTER TABLE `t_cast_explicit_alter_b` CONVERT TO CHARACTER SET utf8";
        memoryTableMeta.console(convertToUtf8, FEATURES);

        SchemaObject table4 = memoryTableMeta.findSchema("d1").findTable("t_cast_explicit_alter_b");
        MySqlCreateTableStatement stmt4 = (MySqlCreateTableStatement) table4.getStatement();

        // Regular column 'raw' should now have utf8 charset
        SQLColumnDefinition rawCol4 = stmt4.findColumn("raw");
        Assert.assertNotNull(rawCol4.getCharsetExpr());
        Assert.assertEquals("utf8", rawCol4.getCharsetExpr().toString());

        // Both generated columns WITHOUT explicit charset should still NOT have explicit column-level charset
        // Because they will use the new table charset (utf8) by default
        SQLColumnDefinition genCol4_1 = stmt4.findColumn("gen_cast_explicit");
        Assert.assertNull("First generated column without explicit charset should NOT have charsetExpr after CONVERT",
            genCol4_1.getCharsetExpr());

        SQLColumnDefinition genCol4_2 = stmt4.findColumn("gen_lcunpk8x");
        Assert.assertNull("Second generated column without explicit charset should NOT have charsetExpr after CONVERT",
            genCol4_2.getCharsetExpr());

        // Table charset should be utf8
        String tableCharset4 = getTableCharset(stmt4);
        Assert.assertEquals("utf8", tableCharset4);

        // Verify output SQL can be re-parsed
        String outputSql4 = stmt4.toString();
        List<SQLStatement> reparsed4 = SQLUtils.parseStatements(outputSql4, DbType.mysql, FEATURES);
        Assert.assertEquals(1, reparsed4.size());

        // ========== Step 5: Add GENERATED column with explicit column-level charset ==========
        String addGenColWithCharset = "ALTER TABLE `t_cast_explicit_alter_b` "
            + "ADD COLUMN `gen_specify` varchar(64) CHARACTER SET gbk "
            + "GENERATED ALWAYS AS (CAST(`id` AS char)) STORED";
        memoryTableMeta.console(addGenColWithCharset, FEATURES);

        SchemaObject table5 = memoryTableMeta.findSchema("d1").findTable("t_cast_explicit_alter_b");
        MySqlCreateTableStatement stmt5 = (MySqlCreateTableStatement) table5.getStatement();

        // New generated column SHOULD have explicit column-level charset (gbk)
        SQLColumnDefinition newGenColWithCharset = stmt5.findColumn("gen_specify");
        Assert.assertNotNull(newGenColWithCharset.getGeneratedAlawsAs());
        Assert.assertNotNull("Generated column with explicit charset SHOULD have column-level charset",
            newGenColWithCharset.getCharsetExpr());
        Assert.assertEquals("gbk", newGenColWithCharset.getCharsetExpr().toString());

        // Other generated columns should still NOT have explicit charset
        SQLColumnDefinition genCol5_1 = stmt5.findColumn("gen_cast_explicit");
        Assert.assertNull("First generated column should NOT have charset", genCol5_1.getCharsetExpr());

        SQLColumnDefinition genCol5_2 = stmt5.findColumn("gen_lcunpk8x");
        Assert.assertNull("Second generated column should NOT have charset", genCol5_2.getCharsetExpr());

        // Regular column should still be utf8
        SQLColumnDefinition rawCol5 = stmt5.findColumn("raw");
        Assert.assertNotNull(rawCol5.getCharsetExpr());
        Assert.assertEquals("utf8", rawCol5.getCharsetExpr().toString());

        // Table charset should still be utf8
        String tableCharset5 = getTableCharset(stmt5);
        Assert.assertEquals("utf8", tableCharset5);

        // Verify final output SQL can be re-parsed
        String finalOutputSql = stmt5.toString();
        List<SQLStatement> reparsedFinal = SQLUtils.parseStatements(finalOutputSql, DbType.mysql, FEATURES);
        Assert.assertEquals(1, reparsedFinal.size());

        // Print final SQL for manual verification
        System.out.println("=== Final CREATE TABLE statement ===");
        System.out.println(finalOutputSql);

        // ========== Step 6: CONVERT TO CHARACTER SET latin1 (verify gen_specify column charset changes) ==========
        String convertToLatin1 = "ALTER TABLE `t_cast_explicit_alter_b` CONVERT TO CHARACTER SET latin1";
        memoryTableMeta.console(convertToLatin1, FEATURES);

        SchemaObject table6 = memoryTableMeta.findSchema("d1").findTable("t_cast_explicit_alter_b");
        MySqlCreateTableStatement stmt6 = (MySqlCreateTableStatement) table6.getStatement();

        // Regular column 'raw' should now have latin1 charset
        SQLColumnDefinition rawCol6 = stmt6.findColumn("raw");
        Assert.assertNotNull(rawCol6.getCharsetExpr());
        Assert.assertEquals("latin1", rawCol6.getCharsetExpr().toString());

        // Generated columns WITHOUT explicit charset should still NOT have explicit column-level charset
        SQLColumnDefinition genCol6_1 = stmt6.findColumn("gen_cast_explicit");
        Assert.assertNull("First generated column should NOT have explicit column-level charset",
            genCol6_1.getCharsetExpr());

        SQLColumnDefinition genCol6_2 = stmt6.findColumn("gen_lcunpk8x");
        Assert.assertNull("Second generated column should NOT have explicit column-level charset",
            genCol6_2.getCharsetExpr());

        // Generated column WITH explicit charset (gen_specify) SHOULD have its column charset changed to latin1
        // But its CAST expression should remain unchanged (without CHARACTER SET, as MySQL doesn't output it)
        SQLColumnDefinition genCol6_3 = stmt6.findColumn("gen_specify");
        Assert.assertNotNull("gen_specify should have explicit column-level charset after CONVERT",
            genCol6_3.getCharsetExpr());
        Assert.assertEquals("latin1", genCol6_3.getCharsetExpr().toString());

        // Verify the CAST expression is present (without CHARACTER SET)
        String genSpecifyExpr6 = genCol6_3.getGeneratedAlawsAs().toString();
        Assert.assertTrue("gen_specify CAST expression should be present",
            genSpecifyExpr6.toLowerCase().contains("cast"));
        Assert.assertFalse("gen_specify CAST expression should NOT contain latin1",
            genSpecifyExpr6.toLowerCase().contains("latin1"));

        // Table charset should be latin1
        String tableCharset6 = getTableCharset(stmt6);
        Assert.assertEquals("latin1", tableCharset6);

        // Verify output SQL can be re-parsed
        String outputSql6 = stmt6.toString();
        System.out.println("=== After CONVERT TO latin1 ===");
        System.out.println(outputSql6);
        List<SQLStatement> reparsed6 = SQLUtils.parseStatements(outputSql6, DbType.mysql, FEATURES);
        Assert.assertEquals(1, reparsed6.size());
    }

    /**
     * Helper method to extract table charset from CREATE TABLE statement options.
     */
    private String getTableCharset(MySqlCreateTableStatement stmt) {
        List<SQLAssignItem> opts = stmt.getTableOptions();
        for (SQLAssignItem item : opts) {
            String target = item.getTarget().toString();
            if (target.contains("CHARSET") || target.contains("CHARACTER")) {
                return item.getValue().toString();
            }
        }
        return null;
    }

    /**
     * Test MySQL behavior: ALTER TABLE ... charset gbk should add CHARACTER SET latin1 to generated columns
     * that have explicit CAST expressions with utf8mb4 charset.
     * <p>
     * MySQL behavior:
     * - Before: gen_cast_explicit has no column-level charset, CAST uses utf8mb4
     * - After ALTER TABLE charset gbk: gen_cast_explicit gets CHARACTER SET latin1, CAST still uses utf8mb4
     */
    @Test
    public void testAlterTableCharsetOnGeneratedColumnsWithCastExplicit() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.setDefaultSchema("d1");

        // ========== Step 1: Create table matching MySQL test case ==========
        String createSql = "CREATE TABLE `t_cast_explicit_alter_b` (\n"
            + "  `id` int NOT NULL,\n"
            + "  `raw` varchar(100) CHARACTER SET latin1 DEFAULT NULL,\n"
            + "  `gen_cast_explicit` varchar(100) GENERATED ALWAYS AS (CAST(`raw` AS char(50) charset utf8mb4)) STORED,\n"
            + "  `gen_lcunpk8x` varchar(64) GENERATED ALWAYS AS (CAST(`id` AS char charset utf8mb4)) STORED,\n"
            + "  `gen_specify` varchar(64) CHARACTER SET latin1 GENERATED ALWAYS AS (CAST(`id` AS char charset utf8mb4)) STORED,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

        memoryTableMeta.console(createSql, FEATURES);
        SchemaObject table1 = memoryTableMeta.findSchema("d1").findTable("t_cast_explicit_alter_b");
        MySqlCreateTableStatement stmt1 = (MySqlCreateTableStatement) table1.getStatement();

        // Verify initial state
        SQLColumnDefinition rawCol1 = stmt1.findColumn("raw");
        Assert.assertNotNull(rawCol1);
        // Note: raw column has explicit CHARACTER SET latin1 in CREATE TABLE
        // Just verify it exists for now

        SQLColumnDefinition genCastExplicit1 = stmt1.findColumn("gen_cast_explicit");
        Assert.assertNotNull(genCastExplicit1.getGeneratedAlawsAs());
        Assert.assertNull("gen_cast_explicit should NOT have explicit column-level charset initially",
            genCastExplicit1.getCharsetExpr());
        // Verify CAST expression contains utf8mb4
        String castExpr1 = genCastExplicit1.getGeneratedAlawsAs().toString();
        Assert.assertTrue("CAST expression should contain utf8mb4",
            castExpr1.toLowerCase().contains("utf8mb4"));

        SQLColumnDefinition genLcunpk8x1 = stmt1.findColumn("gen_lcunpk8x");
        Assert.assertNotNull(genLcunpk8x1.getGeneratedAlawsAs());
        Assert.assertNull("gen_lcunpk8x should NOT have explicit column-level charset initially",
            genLcunpk8x1.getCharsetExpr());

        SQLColumnDefinition genSpecify1 = stmt1.findColumn("gen_specify");
        Assert.assertNotNull(genSpecify1.getGeneratedAlawsAs());
        Assert.assertNotNull("gen_specify should have explicit column-level charset",
            genSpecify1.getCharsetExpr());
        Assert.assertEquals("latin1", genSpecify1.getCharsetExpr().toString());

        String tableCharset1 = getTableCharset(stmt1);
        Assert.assertEquals("latin1", tableCharset1);

        // ========== Step 2: ALTER TABLE charset gbk ==========
        String alterCharset = "ALTER TABLE `t_cast_explicit_alter_b` charset=gbk";
        memoryTableMeta.console(alterCharset, FEATURES);

        SchemaObject table2 = memoryTableMeta.findSchema("d1").findTable("t_cast_explicit_alter_b");
        MySqlCreateTableStatement stmt2 = (MySqlCreateTableStatement) table2.getStatement();

        // After ALTER TABLE charset gbk, MySQL adds CHARACTER SET latin1 to generated columns
        // that don't have explicit charset but have CAST with utf8mb4

        // raw column charset after ALTER TABLE (may or may not have explicit charset)
        SQLColumnDefinition rawCol2 = stmt2.findColumn("raw");
        // We don't enforce raw column's charset here, focus on generated columns

        // gen_cast_explicit should now have CHARACTER SET latin1
        SQLColumnDefinition genCastExplicit2 = stmt2.findColumn("gen_cast_explicit");
        Assert.assertNotNull("gen_cast_explicit should have charset after ALTER TABLE charset",
            genCastExplicit2.getCharsetExpr());
        Assert.assertEquals("latin1", genCastExplicit2.getCharsetExpr().toString());
        // CAST expression should still use utf8mb4
        String castExpr2 = genCastExplicit2.getGeneratedAlawsAs().toString();
        Assert.assertTrue("CAST expression should still contain utf8mb4",
            castExpr2.toLowerCase().contains("utf8mb4"));

        // gen_lcunpk8x should now have CHARACTER SET latin1
        SQLColumnDefinition genLcunpk8x2 = stmt2.findColumn("gen_lcunpk8x");
        Assert.assertNotNull("gen_lcunpk8x should have charset after ALTER TABLE charset",
            genLcunpk8x2.getCharsetExpr());
        Assert.assertEquals("latin1", genLcunpk8x2.getCharsetExpr().toString());
        // CAST expression should still use utf8mb4
        String castExpr2b = genLcunpk8x2.getGeneratedAlawsAs().toString();
        Assert.assertTrue("CAST expression should still contain utf8mb4",
            castExpr2b.toLowerCase().contains("utf8mb4"));

        // gen_specify should still have CHARACTER SET latin1 (unchanged)
        SQLColumnDefinition genSpecify2 = stmt2.findColumn("gen_specify");
        Assert.assertNotNull("gen_specify should still have charset",
            genSpecify2.getCharsetExpr());
        Assert.assertEquals("latin1", genSpecify2.getCharsetExpr().toString());
        // CAST expression should still use utf8mb4
        String castExpr2c = genSpecify2.getGeneratedAlawsAs().toString();
        Assert.assertTrue("CAST expression should still contain utf8mb4",
            castExpr2c.toLowerCase().contains("utf8mb4"));

        // Table charset should be gbk
        String tableCharset2 = getTableCharset(stmt2);
        Assert.assertEquals("gbk", tableCharset2);

        // Verify output SQL can be re-parsed
        String outputSql2 = stmt2.toString();
        System.out.println("=== After ALTER TABLE charset gbk ===");
        System.out.println(outputSql2);
        List<SQLStatement> reparsed2 = SQLUtils.parseStatements(outputSql2, DbType.mysql, FEATURES);
        Assert.assertEquals(1, reparsed2.size());
    }

    /**
     * Test that after ALTER TABLE charset, normalizeTableOptions() and output()
     * should NOT lose the charset added to generated columns.
     * <p>
     * Bug: When executing ALTER TABLE charset, generated columns get original table charset added.
     * But after calling normalizeTableOptions() and output(), the charset is lost.
     */
    @Test
    public void testAlterTableCharsetNormalizeAndOutputGeneratedColumns() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.setDefaultSchema("d1");

        // ========== Step 1: Create table with generated columns ==========
        String createSql = "CREATE TABLE `t_normalize_charset_test` (\n"
            + "  `id` int NOT NULL,\n"
            + "  `raw` varchar(100) CHARACTER SET latin1 DEFAULT NULL,\n"
            + "  `gen_cast_explicit` varchar(100) GENERATED ALWAYS AS (CAST(`raw` AS char(50) charset utf8mb4)) STORED,\n"
            + "  `gen_lcunpk8x` varchar(64) GENERATED ALWAYS AS (CAST(`id` AS char charset utf8mb4)) STORED,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

        memoryTableMeta.console(createSql, FEATURES);
        SchemaObject table1 = memoryTableMeta.findSchema("d1").findTable("t_normalize_charset_test");
        MySqlCreateTableStatement stmt1 = (MySqlCreateTableStatement) table1.getStatement();

        // Verify initial state
        SQLColumnDefinition genCastExplicit1 = stmt1.findColumn("gen_cast_explicit");
        Assert.assertNull("gen_cast_explicit should NOT have explicit column-level charset initially",
            genCastExplicit1.getCharsetExpr());

        SQLColumnDefinition genLcunpk8x1 = stmt1.findColumn("gen_lcunpk8x");
        Assert.assertNull("gen_lcunpk8x should NOT have explicit column-level charset initially",
            genLcunpk8x1.getCharsetExpr());

        // ========== Step 2: ALTER TABLE charset = gbk ==========
        String alterCharset = "ALTER TABLE `t_normalize_charset_test` charset = gbk";
        memoryTableMeta.console(alterCharset, FEATURES);

        SchemaObject table2 = memoryTableMeta.findSchema("d1").findTable("t_normalize_charset_test");
        MySqlCreateTableStatement stmt2 = (MySqlCreateTableStatement) table2.getStatement();

        // After ALTER TABLE charset, generated columns should have latin1 charset added
        SQLColumnDefinition genCastExplicit2 = stmt2.findColumn("gen_cast_explicit");
        Assert.assertNotNull("gen_cast_explicit should have charset after ALTER TABLE charset",
            genCastExplicit2.getCharsetExpr());
        Assert.assertEquals("latin1", genCastExplicit2.getCharsetExpr().toString());

        SQLColumnDefinition genLcunpk8x2 = stmt2.findColumn("gen_lcunpk8x");
        Assert.assertNotNull("gen_lcunpk8x should have charset after ALTER TABLE charset",
            genLcunpk8x2.getCharsetExpr());
        Assert.assertEquals("latin1", genLcunpk8x2.getCharsetExpr().toString());

        System.out.println("=== Before normalizeTableOptions ===");
        System.out.println("gen_cast_explicit charset: " + genCastExplicit2.getCharsetExpr());
        System.out.println("gen_lcunpk8x charset: " + genLcunpk8x2.getCharsetExpr());

        // ========== Step 3: Call normalizeTableOptions() ==========
        stmt2.normalizeTableOptions();

        // ========== Step 4: Output SQL ==========
        StringBuffer data = new StringBuffer(4 * 1024);
        stmt2.output(data);
        data.append("; \n");

        String outputSql = data.toString();
        System.out.println("=== Output SQL after normalizeTableOptions ===");
        System.out.println(outputSql);

        // ========== Step 5: Verify charset is NOT lost ==========
        // The bug is: after normalizeTableOptions() and output(), 
        // the charset added to generated columns is lost

        // Re-parse the output SQL to verify
        List<SQLStatement> reparsed = SQLUtils.parseStatements(outputSql, DbType.mysql, FEATURES);
        Assert.assertEquals(1, reparsed.size());

        MySqlCreateTableStatement reparsedStmt = (MySqlCreateTableStatement) reparsed.get(0);

        // Verify generated columns STILL have charset after normalizeTableOptions + output
        SQLColumnDefinition reparsedGenCastExplicit = reparsedStmt.findColumn("gen_cast_explicit");
        Assert.assertNotNull("gen_cast_explicit should STILL have charset after normalizeTableOptions + output",
            reparsedGenCastExplicit.getCharsetExpr());
        Assert.assertEquals("latin1", reparsedGenCastExplicit.getCharsetExpr().toString());

        SQLColumnDefinition reparsedGenLcunpk8x = reparsedStmt.findColumn("gen_lcunpk8x");
        Assert.assertNotNull("gen_lcunpk8x should STILL have charset after normalizeTableOptions + output",
            reparsedGenLcunpk8x.getCharsetExpr());
        Assert.assertEquals("latin1", reparsedGenLcunpk8x.getCharsetExpr().toString());

        System.out.println("=== Verification passed: charset NOT lost ===");
    }

    /**
     * Test that ALTER TABLE CHARACTER SET and ALTER TABLE charset are equivalent
     * Both should add original table charset to generated columns without explicit charset
     */
    @Test
    public void testAlterTableCharacterSetVsCharset() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.setDefaultSchema("d1");

        // Create table with generated columns
        String createSql = "CREATE TABLE `t_charset_test` (\n"
            + "  `id` int NOT NULL,\n"
            + "  `c_idx` int NOT NULL,\n"
            + "  `raw` varchar(100) DEFAULT NULL,\n"
            + "  `gen_no_charset` varchar(100) GENERATED ALWAYS AS (CAST(`raw` AS char(50) charset utf8mb4)) STORED,\n"
            + "  `gen_with_charset` varchar(64) CHARACTER SET latin1 GENERATED ALWAYS AS (CAST(`id` AS char charset utf8mb4)) STORED,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        memoryTableMeta.console(createSql, FEATURES);

        // Verify initial state
        SchemaObject table0 = memoryTableMeta.findSchema("d1").findTable("t_charset_test");
        MySqlCreateTableStatement stmt0 = (MySqlCreateTableStatement) table0.getStatement();
        System.out.println("=== After CREATE TABLE ===");
        System.out.println("gen_no_charset charset: " + stmt0.findColumn("gen_no_charset").getCharsetExpr());
        System.out.println("gen_with_charset charset: " + stmt0.findColumn("gen_with_charset").getCharsetExpr());
        System.out.println("Table charset: " + getTableCharset(stmt0));

        String ddl2 = "ALTER TABLE t_charset_test " +
            "ADD COLUMN `gen_v1qvnjcuv2f` varchar(64) generated always as (cast(`c_idx` as char)) stored";
        memoryTableMeta.console(ddl2, FEATURES);

        // Verify after ADD COLUMN
        SchemaObject table05 = memoryTableMeta.findSchema("d1").findTable("t_charset_test");
        MySqlCreateTableStatement stmt05 = (MySqlCreateTableStatement) table05.getStatement();
        System.out.println("\n=== After ADD COLUMN gen_v1qvnjcuv2f ===");
        System.out.println("gen_v1qvnjcuv2f charset: " + stmt05.findColumn("gen_v1qvnjcuv2f").getCharsetExpr());
        System.out.println("Table charset: " + getTableCharset(stmt05));

        // Test 1: ALTER TABLE charset=utf8mb4 (same as current)
        String alterCharset = "ALTER TABLE `t_charset_test` charset=utf8mb4";
        memoryTableMeta.console(alterCharset, FEATURES);

        SchemaObject table1 = memoryTableMeta.findSchema("d1").findTable("t_charset_test");
        MySqlCreateTableStatement stmt1 = (MySqlCreateTableStatement) table1.getStatement();
        System.out.println("\n=== After ALTER TABLE charset=utf8mb4 ===");
        System.out.println("gen_v1qvnjcuv2f charset: " + stmt1.findColumn("gen_v1qvnjcuv2f").getCharsetExpr());
        System.out.println("Table charset: " + getTableCharset(stmt1));

        // Test 2: ALTER TABLE charset=utf8 (change charset)
        String alterCharset1 = "ALTER TABLE `t_charset_test` charset=utf8";
        memoryTableMeta.console(alterCharset1, FEATURES);

        SchemaObject table2 = memoryTableMeta.findSchema("d1").findTable("t_charset_test");
        MySqlCreateTableStatement stmt2 = (MySqlCreateTableStatement) table2.getStatement();
        System.out.println("\n=== After ALTER TABLE charset=utf8 ===");
        System.out.println("gen_v1qvnjcuv2f charset: " + stmt2.findColumn("gen_v1qvnjcuv2f").getCharsetExpr());
        System.out.println("Table charset: " + getTableCharset(stmt2));

        // The bug: gen_v1qvnjcuv2f should have charset added (utf8mb4 from original table)
        // but it's missing in the output
        SQLColumnDefinition genV1qvnjcuv2f = stmt2.findColumn("gen_v1qvnjcuv2f");
        Assert.assertNotNull("gen_v1qvnjcuv2f should have charset after ALTER TABLE charset",
            genV1qvnjcuv2f.getCharsetExpr());
        Assert.assertEquals("utf8mb4", genV1qvnjcuv2f.getCharsetExpr().toString());

        Map<String, String> schemaDdls = new HashMap<String, String>();
        StringBuffer data = new StringBuffer(4 * 1024);
        for (Schema schema : memoryTableMeta.getSchemas()) {
            List<String> tableList = schema.showTables();
            for (String tableName : tableList) {
                SchemaObject schemaObject = schema.findTable(tableName);
                SQLStatement statement = schemaObject.getStatement();
                if (statement instanceof MySqlCreateTableStatement) {
                    ((MySqlCreateTableStatement) statement).normalizeTableOptions();
                }
                statement.output(data);
            }
            schemaDdls.put(schema.getName(), data.toString());
        }
        System.out.println("\n=== Final Output DDL ===");
        System.out.println(JSON.toJSONString(schemaDdls));

        // Verify the output DDL contains column-level charset for gen_v1qvnjcuv2f
        String outputDdl = schemaDdls.get("d1");
        Assert.assertTrue("Output DDL should contain 'gen_v1qvnjcuv2f' column-level charset",
            outputDdl.contains("`gen_v1qvnjcuv2f` varchar(64) CHARACTER SET utf8mb4 GENERATED"));
    }

    @Test
    public void testGeneratedColumnAfterTrailingCharsetRoundTrip() {
        SchemaRepository sourceRepository = new SchemaRepository(JdbcConstants.MYSQL);
        sourceRepository.setDefaultSchema("d1");
        sourceRepository.console("CREATE TABLE `t_generated_charset_round_trip` ("
            + "`id` bigint NOT NULL,"
            + "`c_idx` bigint NOT NULL,"
            + "`gen_value` varchar(64) COLLATE utf8mb4_general_ci "
            + "GENERATED ALWAYS AS (CAST(`c_idx` AS char CHARACTER SET utf8mb4)) VIRTUAL,"
            + "PRIMARY KEY (`id`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", FEATURES);
        sourceRepository.console("ALTER TABLE `t_generated_charset_round_trip` "
            + "DEFAULT CHARACTER SET gbk", FEATURES);

        SchemaObject sourceTable = sourceRepository.findSchema("d1").findTable("t_generated_charset_round_trip");
        String snapshot = sourceTable.getStatement().toString();
        Assert.assertTrue(snapshot.contains("COLLATE utf8mb4_general_ci CHARACTER SET utf8mb4 "
            + "GENERATED ALWAYS AS"));

        SchemaRepository reloadedRepository = new SchemaRepository(JdbcConstants.MYSQL);
        reloadedRepository.setDefaultSchema("d1");
        reloadedRepository.console(snapshot, FEATURES);

        SchemaObject reloadedTable =
            reloadedRepository.findSchema("d1").findTable("t_generated_charset_round_trip");
        SQLColumnDefinition generatedColumn =
            ((MySqlCreateTableStatement) reloadedTable.getStatement()).findColumn("gen_value");
        Assert.assertNotNull(generatedColumn.getGeneratedAlawsAs());
        Assert.assertTrue(generatedColumn.isVirtual());

        String reloadedSnapshot = reloadedTable.getStatement().toString();
        Assert.assertTrue(reloadedSnapshot.contains("COLLATE utf8mb4_general_ci CHARACTER SET utf8mb4 "
            + "GENERATED ALWAYS AS"));
        Assert.assertEquals(1, SQLUtils.parseStatements(reloadedSnapshot, DbType.mysql, FEATURES).size());
    }

}
