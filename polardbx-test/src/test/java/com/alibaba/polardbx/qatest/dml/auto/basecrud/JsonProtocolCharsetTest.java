package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.rpc.jdbc.CharsetMapping;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class JsonProtocolCharsetTest extends ReadBaseTestCase {
    private static final String COM_QUERY_TABLE_NAME = "json_protocol_charset_com_query_test";
    private static final String SERVER_PREPARED_TABLE_NAME = "json_protocol_charset_server_prepared_test";
    private static final String PAYLOAD_TABLE_NAME = "json_protocol_charset_payload_test";
    private static final String COMPATIBILITY_MATRIX_TABLE_NAME = "json_protocol_charset_matrix_test";
    private static final String JSON_VALUE =
        "{\"ascii\":\"value\",\"latin\":\"é\",\"nested\":{\"items\":[1,true,null]}}";
    private static final String[] RESULT_CHARSETS = {"utf8mb4", "gbk", "latin1"};
    private static final String[] NO_CONVERSION_RESULT_CHARSETS = {"binary", "null"};
    private static final int MYSQL_TYPE_JSON = 245;
    private static final int BINARY_COLLATION_INDEX = 63;
    private static final int UTF8MB4_COLLATION_INDEX = 45;
    private static final int MYSQL_UTF8MB4_COLLATION_INDEX = 255;
    private static final int GBK_COLLATION_INDEX = 28;
    private static final int POLARDBX_LATIN1_COLLATION_INDEX = 5;
    private static final int MYSQL_LATIN1_COLLATION_INDEX = 8;
    private static final int MYSQL_UTF8MB4_BINARY_COLLATION_INDEX = 46;
    private static final Field FIELDS_FIELD;
    private static final Field MYSQL_TYPE_FIELD;
    private static final Field COLLATION_INDEX_FIELD;

    static {
        try {
            FIELDS_FIELD = com.mysql.jdbc.ResultSetMetaData.class.getDeclaredField("fields");
            FIELDS_FIELD.setAccessible(true);
            Class<?> mysqlFieldClass = FIELDS_FIELD.getType().getComponentType();
            MYSQL_TYPE_FIELD = mysqlFieldClass.getDeclaredField("mysqlType");
            MYSQL_TYPE_FIELD.setAccessible(true);
            COLLATION_INDEX_FIELD = mysqlFieldClass.getDeclaredField("collationIndex");
            COLLATION_INDEX_FIELD.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void requireXProtocol() {
        Assume.assumeTrue("JSON protocol charset compatibility test requires X-Protocol",
            useXproto(tddlConnection));
        JdbcUtil.executeSuccess(tddlConnection, "set names utf8mb4");
    }

    @BeforeClass
    public static void enableJsonResultCharsetCompatibility() {
        setGlobalBooleanVariable("COMPATIBLE_CHARSET_VARIABLES", true);
        setJsonResultCharsetCompatibility(true);
    }

    @AfterClass
    public static void resetJsonResultCharsetCompatibility() {
        setJsonResultCharsetCompatibility(true);
        setGlobalBooleanVariable("COMPATIBLE_CHARSET_VARIABLES", false);
    }

    private void prepareTable(String tableName) {
        prepareTable(tddlConnection, tableName);
    }

    private void prepareTable(Connection connection, String tableName) {
        prepareTable(connection, tableName, JSON_VALUE);
    }

    private void prepareTable(Connection connection, String tableName, String jsonValue) {
        JdbcUtil.executeSuccess(connection, "drop table if exists " + tableName);
        JdbcUtil.executeSuccess(connection, "create table " + tableName
            + " (id bigint primary key, json_col json, text_col varchar(64))");
        JdbcUtil.executeSuccess(connection, "insert into " + tableName
            + " values (1, cast(convert(0x" + toHex(jsonValue.getBytes(StandardCharsets.UTF_8))
            + " using utf8mb4) as json), 'plain-text')");
    }

    private void dropTable(String tableName) {
        dropTable(tddlConnection, tableName);
    }

    private void dropTable(Connection connection, String tableName) {
        JdbcUtil.executeSuccess(connection, "drop table if exists " + tableName);
    }

    @Test
    public void testComQueryJsonMetadataMatchesMysqlColumnAndExpression() throws Exception {
        prepareTable(COM_QUERY_TABLE_NAME);
        try {
            JdbcUtil.executeSuccess(tddlConnection, "set character_set_results = utf8mb4");
            String sql =
                "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select json_col, cast(json_col as json) as cn_json, "
                    + "text_col from " + COM_QUERY_TABLE_NAME;

            try (Statement statement = tddlConnection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
                Assert.assertTrue(resultSet.next());
                Object[] fields = getFields(resultSet.getMetaData());

                assertJsonField(fields[0], BINARY_COLLATION_INDEX);
                assertJsonField(fields[1], UTF8MB4_COLLATION_INDEX);
                Assert.assertNotEquals("VARCHAR metadata must keep a text collation",
                    BINARY_COLLATION_INDEX, getCollationIndex(fields[2]));
                Assert.assertEquals("plain-text", resultSet.getString(3));
            }
        } finally {
            dropTable(COM_QUERY_TABLE_NAME);
        }
    }

    @Test
    public void testServerPreparedJsonMetadataMatchesMysqlColumnAndExpression() throws Exception {
        prepareTable(SERVER_PREPARED_TABLE_NAME);
        try {
            String canonicalJson = queryCanonicalJson(SERVER_PREPARED_TABLE_NAME);
            try (Connection connection = getPolardbxConnectionWithExtraParams(
                "&useServerPrepStmts=true&cachePrepStmts=false&useCursorFetch=false")) {
                JdbcUtil.executeSuccess(connection, "set character_set_results = utf8mb4");
                String sql =
                    "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select json_col, cast(json_col as json) as cn_json "
                        + "from " + SERVER_PREPARED_TABLE_NAME + " where id = ?";

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    assertJsonMetadata(statement.getMetaData(), BINARY_COLLATION_INDEX, BINARY_COLLATION_INDEX);

                    statement.setLong(1, 1L);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        Assert.assertTrue(resultSet.next());
                        assertJsonMetadata(resultSet.getMetaData(), BINARY_COLLATION_INDEX,
                            UTF8MB4_COLLATION_INDEX);
                        byte[] expected = expectedBytes(canonicalJson, "utf8mb4");
                        Assert.assertArrayEquals("Unexpected prepared JSON bytes", expected, resultSet.getBytes(1));
                        Assert.assertArrayEquals("Unexpected prepared JSON expression bytes",
                            expected, resultSet.getBytes(2));
                    }
                }
            }
        } finally {
            dropTable(SERVER_PREPARED_TABLE_NAME);
        }
    }

    @Test
    public void testCompatibilitySwitchOnlyAffectsPhysicalJsonColumn() throws Exception {
        prepareTable(COM_QUERY_TABLE_NAME);
        try {
            JdbcUtil.executeSuccess(tddlConnection, "set character_set_results = utf8mb4");
            setJsonResultCharsetCompatibility(false);
            assertJsonMetadata(
                "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select json_col, cast(json_col as json) as cn_json_off "
                    + "from " + COM_QUERY_TABLE_NAME,
                UTF8MB4_COLLATION_INDEX, UTF8MB4_COLLATION_INDEX);

            setJsonResultCharsetCompatibility(true);
            assertJsonMetadata(
                "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select json_col, cast(json_col as json) as cn_json_on "
                    + "from " + COM_QUERY_TABLE_NAME,
                BINARY_COLLATION_INDEX, UTF8MB4_COLLATION_INDEX);
        } finally {
            setJsonResultCharsetCompatibility(true);
            dropTable(COM_QUERY_TABLE_NAME);
        }
    }

    @Test
    public void testComQueryJsonPayloadFollowsResultCharset() throws Exception {
        prepareTable(PAYLOAD_TABLE_NAME);
        try {
            String canonicalJson = queryCanonicalJson(PAYLOAD_TABLE_NAME);
            String sql = "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select json_col, cast(json_col as json) from "
                + PAYLOAD_TABLE_NAME;

            for (String charset : RESULT_CHARSETS) {
                JdbcUtil.executeSuccess(tddlConnection, "set character_set_results = " + charset);
                try (Statement statement = tddlConnection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)) {
                    Assert.assertTrue(resultSet.next());
                    Object[] fields = getFields(resultSet.getMetaData());
                    assertJsonField(fields[0], BINARY_COLLATION_INDEX);
                    Assert.assertArrayEquals("Unexpected COM_QUERY JSON bytes for " + charset,
                        expectedBytes(canonicalJson, charset), resultSet.getBytes(1));
                }
            }
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, "set character_set_results = utf8mb4");
            dropTable(PAYLOAD_TABLE_NAME);
        }
    }

    @Test
    public void testComQueryJsonUsesUtf8mb4ForNullAndBinaryResultCharset() throws Exception {
        prepareTable(PAYLOAD_TABLE_NAME);
        try {
            String canonicalJson = queryCanonicalJson(PAYLOAD_TABLE_NAME);
            String sql = "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select json_col, cast(json_col as json) from "
                + PAYLOAD_TABLE_NAME;

            for (String charset : NO_CONVERSION_RESULT_CHARSETS) {
                JdbcUtil.executeSuccess(tddlConnection, "set character_set_results = " + charset);
                try (Statement statement = tddlConnection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)) {
                    Assert.assertTrue(resultSet.next());
                    Object[] fields = getFields(resultSet.getMetaData());
                    assertJsonField(fields[0], BINARY_COLLATION_INDEX);
                    assertJsonField(fields[1], BINARY_COLLATION_INDEX);
                    for (int i = 1; i <= 2; i++) {
                        Assert.assertArrayEquals("Unexpected COM_QUERY JSON bytes for " + charset + ",column=" + i,
                            canonicalJson.getBytes(StandardCharsets.UTF_8), resultSet.getBytes(i));
                    }
                }
            }
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, "set character_set_results = utf8mb4");
            dropTable(PAYLOAD_TABLE_NAME);
        }
    }

    @Test
    public void testServerPreparedJsonUsesUtf8mb4ForNullAndBinaryResultCharset() throws Exception {
        prepareTable(SERVER_PREPARED_TABLE_NAME);
        try {
            String canonicalJson = queryCanonicalJson(SERVER_PREPARED_TABLE_NAME);
            for (String charset : NO_CONVERSION_RESULT_CHARSETS) {
                try (Connection connection = getPolardbxConnectionWithExtraParams(
                    "&useServerPrepStmts=true&cachePrepStmts=false&useCursorFetch=false")) {
                    JdbcUtil.executeSuccess(connection, "set character_set_results = " + charset);
                    String sql =
                        "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select json_col, cast(json_col as json) from "
                            + SERVER_PREPARED_TABLE_NAME + " where id = ?";
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setLong(1, 1L);
                        try (ResultSet resultSet = statement.executeQuery()) {
                            Assert.assertTrue(resultSet.next());
                            Object[] fields = getFields(resultSet.getMetaData());
                            assertJsonField(fields[0], BINARY_COLLATION_INDEX);
                            assertJsonField(fields[1], BINARY_COLLATION_INDEX);
                            for (int i = 1; i <= 2; i++) {
                                Assert.assertArrayEquals(
                                    "Unexpected prepared JSON bytes for " + charset + ",column=" + i,
                                    canonicalJson.getBytes(StandardCharsets.UTF_8), resultSet.getBytes(i));
                            }
                        }
                    }
                }
            }
        } finally {
            dropTable(SERVER_PREPARED_TABLE_NAME);
        }
    }

    @Test
    public void testCharacterSetResultsCompatibilityMatrixAgainstMysql() throws Exception {
        prepareTable(tddlConnection, COMPATIBILITY_MATRIX_TABLE_NAME);
        prepareTable(mysqlConnection, COMPATIBILITY_MATRIX_TABLE_NAME);
        final String[][] scenarios = {
            {
                "set_names_utf8mb4", "set names utf8mb4", null,
                Integer.toString(UTF8MB4_COLLATION_INDEX), Integer.toString(MYSQL_UTF8MB4_COLLATION_INDEX),
                Integer.toString(UTF8MB4_COLLATION_INDEX)},
            {
                "set_names_gbk", "set names gbk", null,
                Integer.toString(GBK_COLLATION_INDEX), Integer.toString(GBK_COLLATION_INDEX),
                Integer.toString(GBK_COLLATION_INDEX)},
            {
                "set_names_latin1", "set names latin1", null,
                Integer.toString(POLARDBX_LATIN1_COLLATION_INDEX), Integer.toString(MYSQL_LATIN1_COLLATION_INDEX),
                Integer.toString(POLARDBX_LATIN1_COLLATION_INDEX)},
            {
                "results_utf8mb4_from_gbk", "set names gbk", "set character_set_results = utf8mb4",
                Integer.toString(UTF8MB4_COLLATION_INDEX), Integer.toString(MYSQL_UTF8MB4_COLLATION_INDEX),
                Integer.toString(UTF8MB4_COLLATION_INDEX)},
            {
                "results_gbk_from_utf8mb4", "set names utf8mb4", "set character_set_results = gbk",
                Integer.toString(GBK_COLLATION_INDEX), Integer.toString(GBK_COLLATION_INDEX),
                Integer.toString(GBK_COLLATION_INDEX)},
            {
                "results_null_from_gbk", "set names gbk", "set character_set_results = null",
                Integer.toString(BINARY_COLLATION_INDEX),
                Integer.toString(MYSQL_UTF8MB4_BINARY_COLLATION_INDEX), Integer.toString(BINARY_COLLATION_INDEX)},
            {
                "results_binary_from_gbk", "set names gbk", "set character_set_results = binary",
                Integer.toString(BINARY_COLLATION_INDEX), Integer.toString(BINARY_COLLATION_INDEX),
                Integer.toString(BINARY_COLLATION_INDEX)}
        };
        final String statementSql =
            "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select json_col, cast(json_col as json) as json_expr from "
                + COMPATIBILITY_MATRIX_TABLE_NAME + " where id = 1";
        final String preparedSql =
            "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select json_col, cast(json_col as json) as json_expr from "
                + COMPATIBILITY_MATRIX_TABLE_NAME + " where id = ?";

        final StringBuilder failures = new StringBuilder();
        try {
            for (boolean compatibleCharsetVariables : new boolean[] {false, true}) {
                setGlobalBooleanVariable("COMPATIBLE_CHARSET_VARIABLES", compatibleCharsetVariables);
                try (Connection tddlTextConnection = getPolardbxConnectionWithExtraParams(
                    "&useServerPrepStmts=false");
                    Connection mysqlTextConnection = getMysqlConnectionWithExtraParams(
                        "&useServerPrepStmts=false");
                    Connection tddlPreparedConnection = getPolardbxConnectionWithExtraParams(
                        "&useServerPrepStmts=true&cachePrepStmts=false&useCursorFetch=false");
                    Connection mysqlPreparedConnection = getMysqlConnectionWithExtraParams(
                        "&useServerPrepStmts=true&cachePrepStmts=false&useCursorFetch=false")) {
                    for (String[] scenario : scenarios) {
                        final String label =
                            "compatible=" + compatibleCharsetVariables + ",scenario=" + scenario[0];
                        final int tddlTextExpressionCollation = Integer.parseInt(scenario[3]);
                        final int mysqlExpressionCollation = Integer.parseInt(scenario[4]);
                        final int tddlPreparedExpressionCollation = Integer.parseInt(scenario[5]);

                        applyCharsetScenario(tddlTextConnection, scenario);
                        applyCharsetScenario(mysqlTextConnection, scenario);
                        try {
                            assertStatementResultMatchesMysql(tddlTextConnection, mysqlTextConnection, statementSql,
                                label, tddlTextExpressionCollation, mysqlExpressionCollation);
                        } catch (AssertionError e) {
                            failures.append(e.getMessage()).append('\n');
                        }

                        applyCharsetScenario(tddlPreparedConnection, scenario);
                        applyCharsetScenario(mysqlPreparedConnection, scenario);
                        try {
                            assertPreparedResultMatchesMysql(tddlPreparedConnection, mysqlPreparedConnection,
                                preparedSql, label, tddlPreparedExpressionCollation, mysqlExpressionCollation);
                        } catch (AssertionError e) {
                            failures.append(e.getMessage()).append('\n');
                        }
                    }
                }
            }
            Assert.assertEquals(failures.toString(), 0, failures.length());
        } finally {
            setGlobalBooleanVariable("COMPATIBLE_CHARSET_VARIABLES", true);
            JdbcUtil.executeSuccess(tddlConnection, "set names utf8mb4");
            JdbcUtil.executeSuccess(mysqlConnection, "set names utf8mb4");
            dropTable(tddlConnection, COMPATIBILITY_MATRIX_TABLE_NAME);
            dropTable(mysqlConnection, COMPATIBILITY_MATRIX_TABLE_NAME);
        }
    }

    private void assertStatementResultMatchesMysql(Connection tddl, Connection mysql, String sql, String label,
                                                   int tddlExpressionCollation, int mysqlExpressionCollation)
        throws Exception {
        try (Statement tddlStatement = tddl.createStatement();
            Statement mysqlStatement = mysql.createStatement();
            ResultSet tddlResultSet = tddlStatement.executeQuery(sql);
            ResultSet mysqlResultSet = mysqlStatement.executeQuery(sql)) {
            assertResultMatchesMysql(tddlResultSet, mysqlResultSet, "COM_QUERY," + label,
                tddlExpressionCollation, mysqlExpressionCollation);
        }
    }

    private void assertPreparedResultMatchesMysql(Connection tddl, Connection mysql, String sql, String label,
                                                  int tddlExpressionCollation, int mysqlExpressionCollation)
        throws Exception {
        try (PreparedStatement tddlStatement = tddl.prepareStatement(sql);
            PreparedStatement mysqlStatement = mysql.prepareStatement(sql)) {
            tddlStatement.setLong(1, 1L);
            mysqlStatement.setLong(1, 1L);
            try (ResultSet tddlResultSet = tddlStatement.executeQuery();
                ResultSet mysqlResultSet = mysqlStatement.executeQuery()) {
                assertResultMatchesMysql(tddlResultSet, mysqlResultSet, "COM_STMT_EXECUTE," + label,
                    tddlExpressionCollation, mysqlExpressionCollation);
            }
        }
    }

    private static void assertResultMatchesMysql(ResultSet tddlResultSet, ResultSet mysqlResultSet, String label,
                                                 int tddlExpressionCollation, int mysqlExpressionCollation)
        throws Exception {
        Assert.assertTrue(label, tddlResultSet.next());
        Assert.assertTrue(label, mysqlResultSet.next());
        Object[] tddlFields = getFields(tddlResultSet.getMetaData());
        Object[] mysqlFields = getFields(mysqlResultSet.getMetaData());
        Assert.assertEquals(label, mysqlFields.length, tddlFields.length);
        for (int i = 0; i < mysqlFields.length; i++) {
            Assert.assertEquals(label + ",column=" + i, MYSQL_TYPE_JSON, MYSQL_TYPE_FIELD.getInt(mysqlFields[i]));
            Assert.assertEquals(label + ",column=" + i, MYSQL_TYPE_JSON, MYSQL_TYPE_FIELD.getInt(tddlFields[i]));
            if (i == 0) {
                Assert.assertEquals(label + ",MySQL physical JSON charsetIndex", BINARY_COLLATION_INDEX,
                    getCollationIndex(mysqlFields[i]));
                Assert.assertEquals(label + ",PolarDB-X physical JSON charsetIndex", BINARY_COLLATION_INDEX,
                    getCollationIndex(tddlFields[i]));
            } else {
                assertCharsetMatches(label + ",MySQL JSON expression charset", mysqlExpressionCollation,
                    getCollationIndex(mysqlFields[i]));
                assertCharsetMatches(label + ",PolarDB-X JSON expression charset", tddlExpressionCollation,
                    getCollationIndex(tddlFields[i]));
            }
            Assert.assertArrayEquals(label + ",payload,column=" + i,
                mysqlResultSet.getBytes(i + 1), tddlResultSet.getBytes(i + 1));
        }
        Assert.assertFalse(label, tddlResultSet.next());
        Assert.assertFalse(label, mysqlResultSet.next());
    }

    private static void assertCharsetMatches(String label, int expectedCollationIndex, int actualCollationIndex) {
        final String expectedCharset =
            CharsetMapping.getMysqlCharsetNameForCollationIndex(expectedCollationIndex);
        final String actualCharset = CharsetMapping.getMysqlCharsetNameForCollationIndex(actualCollationIndex);
        Assert.assertNotNull(label + ",unknown expected collation=" + expectedCollationIndex, expectedCharset);
        Assert.assertNotNull(label + ",unknown actual collation=" + actualCollationIndex, actualCharset);
        Assert.assertEquals(label, expectedCharset, actualCharset);
    }

    private static void applyCharsetScenario(Connection connection, String[] scenario) {
        JdbcUtil.executeSuccess(connection, scenario[1]);
        if (scenario[2] != null) {
            JdbcUtil.executeSuccess(connection, scenario[2]);
        }
    }

    private static void setJsonResultCharsetCompatibility(boolean enabled) {
        setGlobalBooleanVariable("ENABLE_JSON_RESULT_CHARSET_COMPATIBILITY", enabled);
    }

    private static void setGlobalBooleanVariable(String variable, boolean enabled) {
        JdbcUtil.executeSuccess(getPolardbxConnection0(),
            "set global " + variable + " = " + enabled);
        waitForDynamicConfig();
    }

    private static void waitForDynamicConfig() {
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private String queryCanonicalJson(String tableName) throws Exception {
        JdbcUtil.executeSuccess(tddlConnection, "set character_set_results = utf8mb4");
        try (Statement statement = tddlConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                "/*+TDDL:ENABLE_PUSH_PROJECT=false*/ select json_col from " + tableName)) {
            Assert.assertTrue(resultSet.next());
            return resultSet.getString(1);
        }
    }

    private static byte[] expectedBytes(String value, String mysqlCharset) {
        if (isUtf8mb4(mysqlCharset)) {
            return value.getBytes(StandardCharsets.UTF_8);
        }
        if ("latin1".equalsIgnoreCase(mysqlCharset)) {
            return value.getBytes(Charset.forName("Cp1252"));
        }
        return value.getBytes(Charset.forName(mysqlCharset));
    }

    private static boolean isUtf8mb4(String mysqlCharset) {
        return "utf8mb4".equalsIgnoreCase(mysqlCharset);
    }

    private static String toHex(byte[] bytes) {
        char[] hex = new char[bytes.length * 2];
        final char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            hex[i * 2] = digits[(bytes[i] >>> 4) & 0x0f];
            hex[i * 2 + 1] = digits[bytes[i] & 0x0f];
        }
        return new String(hex);
    }

    private static void assertJsonMetadata(ResultSetMetaData metaData, int columnCollation, int expressionCollation)
        throws IllegalAccessException {
        Object[] fields = getFields(metaData);
        Assert.assertEquals(2, fields.length);
        assertJsonField(fields[0], columnCollation);
        assertJsonField(fields[1], expressionCollation);
    }

    private void assertJsonMetadata(String sql, int columnCollation, int expressionCollation) throws Exception {
        try (Statement statement = tddlConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)) {
            Assert.assertTrue(resultSet.next());
            Object[] fields = getFields(resultSet.getMetaData());
            Assert.assertEquals(2, fields.length);
            assertJsonField(fields[0], columnCollation);
            assertJsonField(fields[1], expressionCollation);
        }
    }

    private static Object[] getFields(ResultSetMetaData metaData) throws IllegalAccessException {
        Assert.assertTrue("Expected Connector/J 5.1 metadata but got " + metaData.getClass(),
            metaData instanceof com.mysql.jdbc.ResultSetMetaData);
        return (Object[]) FIELDS_FIELD.get(metaData);
    }

    private static void assertJsonField(Object field, int expectedCollationIndex) throws IllegalAccessException {
        Assert.assertEquals("JSON column must use MYSQL_TYPE_JSON", MYSQL_TYPE_JSON, MYSQL_TYPE_FIELD.getInt(field));
        Assert.assertEquals("Unexpected JSON collation metadata",
            expectedCollationIndex, getCollationIndex(field));
    }

    private static int getCollationIndex(Object field) throws IllegalAccessException {
        return COLLATION_INDEX_FIELD.getInt(field);
    }

}
