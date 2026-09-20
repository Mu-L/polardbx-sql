package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.alibaba.polardbx.qatest.util.JdbcUtil.executeSuccess;

public class UpsertEnumTest extends AutoCrudBasedLockTestCase {

    private static final String TABLE_NAME = "all_type";

    private Connection tddlConnection;

    @Before
    public void setUp() throws Exception {
        tddlConnection = getPolardbxConnection();
        dropTable();
        createTable();
    }

    @After
    public void tearDown() throws Exception {
        dropTable();
        if (tddlConnection != null) {
            tddlConnection.close();
        }
    }

    private void createTable() throws SQLException {
        String sql = "CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n" +
            "    `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "    `uk` bigint NOT NULL,\n" +
            "    `c_bit_1` bit(1) NOT NULL DEFAULT 0x1,\n" +
            "    `c_bit_8` bit(8) NOT NULL DEFAULT 0xFF,\n" +
            "    `c_bit_16` bit(16) NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_bit_32` bit(32) NOT NULL DEFAULT 0xFFFFFFFF,\n" +
            "    `c_bit_64` bit(64) NOT NULL DEFAULT 0xFFFFFFFFFFFFFFFF,\n" +
            "    `c_bit_hex_8` bit(8) NOT NULL DEFAULT 0xFF,\n" +
            "    `c_bit_hex_16` bit(16) NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_bit_hex_32` bit(32) NOT NULL DEFAULT 0xFFFFFFFF,\n" +
            "    `c_bit_hex_64` bit(64) NOT NULL DEFAULT 0xFFFFFFFFFFFFFFFF,\n" +
            "    `c_boolean` tinyint(1) NOT NULL DEFAULT '1',\n" +
            "    `c_boolean_2` tinyint(1) NOT NULL DEFAULT '0',\n" +
            "    `c_boolean_3` tinyint(1) NOT NULL DEFAULT '0',\n" +
            "    `c_tinyint_1` tinyint(1) NOT NULL DEFAULT '127',\n" +
            "    `c_tinyint_4` tinyint NOT NULL DEFAULT '-128',\n" +
            "    `c_tinyint_8` tinyint NOT NULL DEFAULT '-75',\n" +
            "    `c_tinyint_3_un` tinyint UNSIGNED NOT NULL DEFAULT '0',\n" +
            "    `c_tinyint_8_un` tinyint UNSIGNED NOT NULL DEFAULT '255',\n" +
            "    `c_tinyint_df_un` tinyint UNSIGNED NOT NULL DEFAULT '255',\n" +
            "    `c_tinyint_zerofill_un` tinyint(3) UNSIGNED ZEROFILL NOT NULL DEFAULT '255',\n" +
            "    `c_smallint_1` smallint NOT NULL DEFAULT '14497',\n" +
            "    `c_smallint_2` smallint NOT NULL DEFAULT '111',\n" +
            "    `c_smallint_6` smallint NOT NULL DEFAULT '-32768',\n" +
            "    `c_smallint_16` smallint NOT NULL DEFAULT '32767',\n" +
            "    `c_smallint_16_un` smallint UNSIGNED NOT NULL DEFAULT '65535',\n" +
            "    `c_smallint_df_un` smallint UNSIGNED NOT NULL DEFAULT '65535',\n" +
            "    `c_smallint_zerofill_un` smallint(5) UNSIGNED ZEROFILL NOT NULL DEFAULT '65535',\n" +
            "    `c_mediumint_1` mediumint NOT NULL DEFAULT '-8388608',\n" +
            "    `c_mediumint_3` mediumint NOT NULL DEFAULT '3456789',\n" +
            "    `c_mediumint_9` mediumint NOT NULL DEFAULT '8388607',\n" +
            "    `c_mediumint_24` mediumint NOT NULL DEFAULT '-1845105',\n" +
            "    `c_mediumint_8_un` mediumint UNSIGNED NOT NULL DEFAULT '16777215',\n" +
            "    `c_mediumint_24_un` mediumint UNSIGNED NOT NULL DEFAULT '16777215',\n" +
            "    `c_mediumint_df_un` mediumint UNSIGNED NOT NULL DEFAULT '16777215',\n" +
            "    `c_mediumint_zerofill_un` mediumint(8) UNSIGNED ZEROFILL NOT NULL DEFAULT '00007788',\n" +
            "    `c_int_1` int NOT NULL DEFAULT '-2147483648',\n" +
            "    `c_int_4` int NOT NULL DEFAULT '872837',\n" +
            "    `c_int_11` int NOT NULL DEFAULT '2147483647',\n" +
            "    `c_int_32` int NOT NULL DEFAULT '-2147483648',\n" +
            "    `c_int_32_un` int UNSIGNED NOT NULL DEFAULT '4294967295',\n" +
            "    `c_int_df_un` int UNSIGNED NOT NULL DEFAULT '4294967295',\n" +
            "    `c_int_zerofill_un` int(10) UNSIGNED ZEROFILL NOT NULL DEFAULT '4294967295',\n" +
            "    `c_bigint_1` bigint NOT NULL DEFAULT '-816854218224922624',\n" +
            "    `c_bigint_20` bigint NOT NULL DEFAULT '-9223372036854775808',\n" +
            "    `c_bigint_64` bigint NOT NULL DEFAULT '9223372036854775807',\n" +
            "    `c_bigint_20_un` bigint UNSIGNED NOT NULL DEFAULT '9223372036854775808',\n" +
            "    `c_bigint_64_un` bigint UNSIGNED NOT NULL DEFAULT '18446744073709551615',\n" +
            "    `c_bigint_df_un` bigint UNSIGNED NOT NULL DEFAULT '18446744073709551615',\n" +
            "    `c_bigint_zerofill_un` bigint(20) UNSIGNED ZEROFILL NOT NULL DEFAULT '00000000000000000001',\n" +
            "    `c_tinyint_hex_1` tinyint(1) NOT NULL DEFAULT 0x3F,\n" +
            "    `c_tinyint_hex_4` tinyint NOT NULL DEFAULT 0x4F,\n" +
            "    `c_tinyint_hex_8` tinyint NOT NULL DEFAULT 0x5F,\n" +
            "    `c_tinyint_hex_3_un` tinyint UNSIGNED NOT NULL DEFAULT 0x2F,\n" +
            "    `c_tinyint_hex_8_un` tinyint UNSIGNED NOT NULL DEFAULT 0x4E,\n" +
            "    `c_tinyint_hex_zerofill_un` tinyint(3) UNSIGNED ZEROFILL NOT NULL DEFAULT 0x3F,\n" +
            "    `c_smallint_hex_1` smallint NOT NULL DEFAULT 0x2FFF,\n" +
            "    `c_smallint_hex_2` smallint NOT NULL DEFAULT 0x3FFF,\n" +
            "    `c_smallint_hex_6` smallint NOT NULL DEFAULT 0x4FEF,\n" +
            "    `c_smallint_hex_16` smallint NOT NULL DEFAULT 0x5EDF,\n" +
            "    `c_smallint_hex_16_un` smallint UNSIGNED NOT NULL DEFAULT 0x7EDF,\n" +
            "    `c_smallint_hex_zerofill_un` smallint(5) UNSIGNED ZEROFILL NOT NULL DEFAULT 0x8EFF,\n" +
            "    `c_mediumint_hex_1` mediumint NOT NULL DEFAULT 0x9EEE,\n" +
            "    `c_mediumint_hex_3` mediumint NOT NULL DEFAULT 0x7DDD,\n" +
            "    `c_mediumint_hex_9` mediumint NOT NULL DEFAULT 0x6CCC,\n" +
            "    `c_mediumint_hex_24` mediumint NOT NULL DEFAULT 0x5FCC,\n" +
            "    `c_mediumint_hex_8_un` mediumint UNSIGNED NOT NULL DEFAULT 0xFCFF,\n" +
            "    `c_mediumint_hex_24_un` mediumint UNSIGNED NOT NULL DEFAULT 0xFCFF,\n" +
            "    `c_mediumint_hex_zerofill_un` mediumint(8) UNSIGNED ZEROFILL NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_int_hex_1` int NOT NULL DEFAULT 0xFFFFFF,\n" +
            "    `c_int_hex_4` int NOT NULL DEFAULT 0xEFFFFF,\n" +
            "    `c_int_hex_11` int NOT NULL DEFAULT 0xEEFFFF,\n" +
            "    `c_int_hex_32` int NOT NULL DEFAULT 0xEEFFFF,\n" +
            "    `c_int_hex_32_un` int UNSIGNED NOT NULL DEFAULT 0xFFEEFF,\n" +
            "    `c_int_hex_zerofill_un` int(10) UNSIGNED ZEROFILL NOT NULL DEFAULT 0xFFEEFF,\n" +
            "    `c_bigint_hex_1` bigint NOT NULL DEFAULT 0xFEFFFFFFFEFFFF,\n" +
            "    `c_bigint_hex_20` bigint NOT NULL DEFAULT 0xFFFFFFFFFEFFFF,\n" +
            "    `c_bigint_hex_64` bigint NOT NULL DEFAULT 0xEFFFFFFFFEFFFF,\n" +
            "    `c_bigint_hex_20_un` bigint UNSIGNED NOT NULL DEFAULT 0xCFFFFFFFFEFFFF,\n" +
            "    `c_bigint_hex_64_un` bigint UNSIGNED NOT NULL DEFAULT 0xAFFFFFFFFEFFFF,\n" +
            "    `c_bigint_hex_zerofill_un` bigint(20) UNSIGNED ZEROFILL NOT NULL DEFAULT 0x01,\n" +
            "    `c_decimal_hex` decimal(10, 0) NOT NULL DEFAULT 0xFFFFFF,\n" +
            "    `c_decimal_hex_pr` decimal(10, 3) NOT NULL DEFAULT 0xEFFF,\n" +
            "    `c_decimal_hex_un` decimal(10, 0) UNSIGNED NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_float_hex` float NOT NULL DEFAULT 0xEFFF,\n" +
            "    `c_float_hex_pr` float(10, 3) NOT NULL DEFAULT 0xEEEE,\n" +
            "    `c_float_hex_un` float(10, 3) UNSIGNED NOT NULL DEFAULT 0xFFEF,\n" +
            "    `c_double_hex` double NOT NULL DEFAULT 0xFFFFEFFF,\n" +
            "    `c_double_hex_pr` double(10, 3) NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_double_hex_un` double(10, 3) UNSIGNED NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_tinyint_hex_x_1` tinyint(1) NOT NULL DEFAULT 0x1F,\n" +
            "    `c_tinyint_hex_x_4` tinyint NOT NULL DEFAULT 0x2F,\n" +
            "    `c_tinyint_hex_x_8` tinyint NOT NULL DEFAULT 0x3F,\n" +
            "    `c_tinyint_hex_x_3_un` tinyint UNSIGNED NOT NULL DEFAULT 0xFF,\n" +
            "    `c_tinyint_hex_x_8_un` tinyint UNSIGNED NOT NULL DEFAULT 0xEE,\n" +
            "    `c_tinyint_hex_x_zerofill_un` tinyint(3) UNSIGNED ZEROFILL NOT NULL DEFAULT 0xFF,\n" +
            "    `c_smallint_hex_x_1` smallint NOT NULL DEFAULT 0x1FFF,\n" +
            "    `c_smallint_hex_x_2` smallint NOT NULL DEFAULT 0x1FFF,\n" +
            "    `c_smallint_hex_x_6` smallint NOT NULL DEFAULT 0x2FEF,\n" +
            "    `c_smallint_hex_x_16` smallint NOT NULL DEFAULT 0x1EDF,\n" +
            "    `c_smallint_hex_x_16_un` smallint UNSIGNED NOT NULL DEFAULT 0x1EDF,\n" +
            "    `c_smallint_hex_x_zerofill_un` smallint(5) UNSIGNED ZEROFILL NOT NULL DEFAULT 0x5EFF,\n" +
            "    `c_mediumint_hex_x_1` mediumint NOT NULL DEFAULT 0x4EEE,\n" +
            "    `c_mediumint_hex_x_3` mediumint NOT NULL DEFAULT 0x3DDD,\n" +
            "    `c_mediumint_hex_x_9` mediumint NOT NULL DEFAULT 0x2CCC,\n" +
            "    `c_mediumint_hex_x_24` mediumint NOT NULL DEFAULT 0x1FCC,\n" +
            "    `c_mediumint_hex_x_8_un` mediumint UNSIGNED NOT NULL DEFAULT 0xFAFF,\n" +
            "    `c_mediumint_hex_x_24_un` mediumint UNSIGNED NOT NULL DEFAULT 0xFAFF,\n" +
            "    `c_mediumint_hex_x_zerofill_un` mediumint(8) UNSIGNED ZEROFILL NOT NULL DEFAULT 0xFAFF,\n" +
            "    `c_int_hex_x_1` int NOT NULL DEFAULT 0xFFFFFF,\n" +
            "    `c_int_hex_x_4` int NOT NULL DEFAULT 0xFFFFFF,\n" +
            "    `c_int_hex_x_11` int NOT NULL DEFAULT 0xFFFFFF,\n" +
            "    `c_int_hex_x_32` int NOT NULL DEFAULT 0xFFFFFF,\n" +
            "    `c_int_hex_x_32_un` int UNSIGNED NOT NULL DEFAULT 0xFFFFFF,\n" +
            "    `c_int_hex_x_zerofill_un` int(10) UNSIGNED ZEROFILL NOT NULL DEFAULT 0xFFFFFF,\n" +
            "    `c_bigint_hex_x_1` bigint NOT NULL DEFAULT 0xFFFFFFFFFFFFFF,\n" +
            "    `c_bigint_hex_x_20` bigint NOT NULL DEFAULT 0xFFFFFFFFFFFFFF,\n" +
            "    `c_bigint_hex_x_64` bigint NOT NULL DEFAULT 0xFFFFFFFFFFFFFF,\n" +
            "    `c_bigint_hex_x_20_un` bigint UNSIGNED NOT NULL DEFAULT 0xFFFFFFFFFFFFFF,\n" +
            "    `c_bigint_hex_x_64_un` bigint UNSIGNED NOT NULL DEFAULT 0xFFFFFFFFFFFFFF,\n" +
            "    `c_bigint_hex_x_zerofill_un` bigint(20) UNSIGNED ZEROFILL NOT NULL DEFAULT 0xF1AB,\n" +
            "    `c_decimal_hex_x` decimal(10, 0) NOT NULL DEFAULT 0xFFFFFFFF,\n" +
            "    `c_decimal_hex_x_pr` decimal(10, 3) NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_decimal_hex_x_un` decimal(10, 0) UNSIGNED NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_float_hex_x` float NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_float_hex_x_pr` float(10, 3) NOT NULL DEFAULT 0xEEEE,\n" +
            "    `c_float_hex_x_un` float(10, 3) UNSIGNED NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_double_hex_x` double NOT NULL DEFAULT 0xFFFFEFFF,\n" +
            "    `c_double_hex_x_pr` double(10, 3) NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_double_hex_x_un` double(10, 3) UNSIGNED NOT NULL DEFAULT 0xFFFF,\n" +
            "    `c_decimal` decimal(10, 0) NOT NULL DEFAULT '-1613793319',\n" +
            "    `c_decimal_pr` decimal(10, 3) NOT NULL DEFAULT '1223077.292',\n" +
            "    `c_decimal_un` decimal(10, 0) UNSIGNED NOT NULL DEFAULT '10234273',\n" +
            "    `c_numeric_df` decimal(10, 0) NOT NULL DEFAULT '10234273',\n" +
            "    `c_numeric_10` decimal(10, 5) NOT NULL DEFAULT '1.00000',\n" +
            "    `c_numeric_df_un` decimal(10, 0) UNSIGNED NOT NULL DEFAULT '10234273',\n" +
            "    `c_numeric_un` decimal(10, 6) UNSIGNED NOT NULL DEFAULT '1.000000',\n" +
            "    `c_dec_df` decimal(10, 0) NOT NULL DEFAULT '10234273',\n" +
            "    `c_dec_10` decimal(10, 5) NOT NULL DEFAULT '1.00000',\n" +
            "    `c_dec_df_un` decimal(10, 0) UNSIGNED NOT NULL DEFAULT '10234273',\n" +
            "    `c_dec_un` decimal(10, 6) UNSIGNED NOT NULL DEFAULT '1.000000',\n" +
            "    `c_float` float NOT NULL DEFAULT '910963000',\n" +
            "    `c_float_pr` float(10, 3) NOT NULL DEFAULT '-5839673.500',\n" +
            "    `c_float_un` float(10, 3) UNSIGNED NOT NULL DEFAULT '2648.644',\n" +
            "    `c_double` double NOT NULL DEFAULT '4334081673.614155',\n" +
            "    `c_double_pr` double(10, 3) NOT NULL DEFAULT '6973286.176',\n" +
            "    `c_double_un` double(10, 3) UNSIGNED NOT NULL DEFAULT '7630560.182',\n" +
            "    `c_date` date NOT NULL DEFAULT '2019-02-15' COMMENT 'date',\n" +
            "    `c_datetime` datetime NOT NULL DEFAULT '2019-02-15 14:54:41',\n" +
            "    `c_datetime_ms` datetime(3) NOT NULL DEFAULT '2019-02-15 14:54:41.789',\n" +
            "    `c_datetime_df` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
            "    `c_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
            "    `c_timestamp_2` timestamp NOT NULL DEFAULT '2020-12-29 12:27:30',\n" +
            "    `c_time` time NOT NULL DEFAULT '20:12:46',\n" +
            "    `c_time_3` time(3) NOT NULL DEFAULT '12:30:00.000',\n" +
            "    `c_year` year NOT NULL DEFAULT '2019',\n" +
            "    `c_year_4` year NOT NULL DEFAULT '2029',\n" +
            "    `c_char` char(50) NOT NULL DEFAULT 'sjdlfjsdljldfjsldfsd',\n" +
            "    `c_char_df` char(1) NOT NULL DEFAULT 'x',\n" +
            "    `c_varchar` varchar(50) NOT NULL DEFAULT 'sjdlfjsldhgowuere',\n" +
            "    `c_nchar` char(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '你好',\n" +
            "    `c_nvarchar` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '北京',\n"
            +
            "    `c_binary_df` binary(1) NOT NULL DEFAULT 0x78,\n" +
            "    `c_binary` binary(200) NOT NULL DEFAULT 'qoeuroieshdfs',\n" +
            "    `c_varbinary` varbinary(200) NOT NULL DEFAULT 'sdfjsljlewwfs',\n" +
            "    `c_enum` enum('a', 'b', 'c') NOT NULL DEFAULT 'a',\n" +
            "    `c_enum_2` enum('x-small', 'small', 'medium', 'large', 'x-large') NOT NULL DEFAULT 'small',\n" +
            "    `c_set` set('a', 'b', 'c') NOT NULL DEFAULT 'a',\n" +
            "    `c_idx` bigint NOT NULL DEFAULT '100',\n" +
            "    `access_counter` int NOT NULL DEFAULT '0',\n" +
            "    PRIMARY KEY (`id`),\n" +
            "    UNIQUE GLOBAL INDEX `uk` (`uk`) PARTITION BY KEY(`uk`) PARTITIONS 3,\n" +
            "    UNIQUE LOCAL KEY `_local_uk` (`uk`)\n" +
            ") ENGINE = InnoDB AUTO_INCREMENT = 10007 DEFAULT CHARSET = gbk PARTITION BY KEY(`id`) PARTITIONS 3;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void dropTable() {
        String sql = "DROP TABLE IF EXISTS " + TABLE_NAME;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testUpsertWithEnum() throws SQLException {
        // upsert twice
        String sql = "/*+TDDL:cmd_extra()*/ INSERT INTO " + TABLE_NAME
            + " (uk) VALUES (1001) ON DUPLICATE KEY UPDATE access_counter = access_counter + 1, c_datetime_df = CURRENT_TIMESTAMP, c_timestamp = CURRENT_TIMESTAMP";
        executeSuccess(tddlConnection, sql);
        executeSuccess(tddlConnection, sql);
    }
}