package com.alibaba.polardbx.qatest.dml.sharding.externalized;

import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Production-path integration coverage for externalized columns in a legacy DRDS database.
 */
@RunWith(CommonCaseRunner.class)
public class ExternalizedColumnDrdsTest extends ExternalizedColumnTestBase {

    private static final String CLASS_DB = "ext_drds_" + randomSuffix();
    private static final String TABLE_NAME = "ext_drds_smoke";

    public ExternalizedColumnDrdsTest() {
        super(DatabaseMode.DRDS);
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        createIsolatedDatabase(CLASS_DB, "drds");
    }

    @AfterClass
    public static void afterClass() {
        dropIsolatedDatabase(CLASS_DB);
    }

    @Before
    public void before() throws SQLException {
        tddlConnection = getTpConnection(CLASS_DB);
        JdbcUtil.executeUpdate(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME + " PURGE");
    }

    @After
    public void after() throws SQLException {
        if (tddlConnection != null) {
            try {
                JdbcUtil.executeUpdate(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME + " PURGE");
            } finally {
                tddlConnection.close();
                tddlConnection = null;
            }
        }
    }

    @Test
    public void testCreateCrudShowCreateAndDrop() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT EXTERNALIZE"
                + ") DBPARTITION BY HASH(id) TBPARTITION BY HASH(id) TBPARTITIONS 2");

        assertMappingCount("PUBLIC", 1);

        tddlConnection.setAutoCommit(false);
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "INSERT INTO " + TABLE_NAME + " (id, content) VALUES (?, ?)")) {
            ps.setLong(1, 1L);
            ps.setString(2, "drds externalized value");
            assertEquals(1, ps.executeUpdate());

            ps.setLong(1, 2L);
            ps.setNull(2, java.sql.Types.LONGVARCHAR);
            assertEquals(1, ps.executeUpdate());
            tddlConnection.commit();
        } finally {
            tddlConnection.setAutoCommit(true);
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, content FROM " + TABLE_NAME + " ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertEquals("drds externalized value", rs.getString(2));
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong(1));
            assertNull(rs.getString(2));
            assertFalse(rs.next());
        }

        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "UPDATE " + TABLE_NAME + " SET content = ? WHERE id = ?")) {
            ps.setString(1, "updated drds value");
            ps.setLong(2, 1L);
            assertEquals(1, ps.executeUpdate());
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT content FROM " + TABLE_NAME + " WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("updated drds value", rs.getString(1));
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW CREATE TABLE " + TABLE_NAME)) {
            assertTrue(rs.next());
            String showCreate = rs.getString(2).toLowerCase(Locale.ROOT);
            assertTrue(showCreate.contains("`content` longtext externalize"));
            assertFalse(showCreate.contains("content_addr_"));
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE " + TABLE_NAME + " PURGE");
        assertMappingCount("PUBLIC", 0);
        assertMappingCountAtLeast("DROP", 1);
    }

    @Test
    public void testRejectExternalizedShardKey() {
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT EXTERNALIZE"
                + ") DBPARTITION BY HASH(content)",
            "cannot be a DBPARTITION or TBPARTITION key");
    }

    @Test
    public void testRejectDropWithoutPurge() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT EXTERNALIZE"
                + ") DBPARTITION BY HASH(id)");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "DROP TABLE " + TABLE_NAME,
            "externalized DRDS table requires PURGE");
        assertMappingCount("PUBLIC", 1);
    }

    @Test
    public void testModifyColumnExternalize() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "content LONGTEXT"
                + ") DBPARTITION BY HASH(id) TBPARTITION BY HASH(id) TBPARTITIONS 2");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (1, 'before mce'), (2, NULL)");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE_NAME + " MODIFY COLUMN content LONGTEXT EXTERNALIZE");

        assertMappingCount("PUBLIC", 1);
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, content FROM " + TABLE_NAME + " ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertEquals("before mce", rs.getString(2));
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong(1));
            assertNull(rs.getString(2));
            assertFalse(rs.next());
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + TABLE_NAME + " SET content = 'after mce' WHERE id = 1");
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT content FROM " + TABLE_NAME + " WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("after mce", rs.getString(1));
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW CREATE TABLE " + TABLE_NAME)) {
            assertTrue(rs.next());
            String showCreate = rs.getString(2).toLowerCase(Locale.ROOT);
            assertTrue(showCreate.contains("`content` longtext externalize"));
            assertFalse(showCreate.contains("content_addr_"));
        }
    }

    @Test
    public void testCreateAllSupportedTypesAndInformationSchema() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "c_text TEXT EXTERNALIZE,"
                + "c_tinytext TINYTEXT EXTERNALIZE,"
                + "c_mediumtext MEDIUMTEXT EXTERNALIZE,"
                + "c_longtext LONGTEXT EXTERNALIZE,"
                + "c_blob BLOB EXTERNALIZE,"
                + "c_tinyblob TINYBLOB EXTERNALIZE,"
                + "c_mediumblob MEDIUMBLOB EXTERNALIZE,"
                + "c_longblob LONGBLOB EXTERNALIZE"
                + ") DBPARTITION BY HASH(id) TBPARTITION BY HASH(id) TBPARTITIONS 2");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns"
                + " WHERE table_schema=DATABASE() AND table_name='" + TABLE_NAME + "'"
                + " ORDER BY ordinal_position")) {
            int externalizedColumns = 0;
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                assertFalse("Physical address columns must stay hidden", columnName.endsWith("_addr_"));
                if (!"id".equalsIgnoreCase(columnName)) {
                    externalizedColumns++;
                    assertTrue(rs.getString("DATA_TYPE").toLowerCase(Locale.ROOT).contains("text")
                        || rs.getString("DATA_TYPE").toLowerCase(Locale.ROOT).contains("blob"));
                }
            }
            assertEquals(8, externalizedColumns);
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT COUNT(*) FROM metadb.ext_column_mapping"
                + " WHERE table_schema=DATABASE() AND table_name='" + TABLE_NAME + "'"
                + " AND status='PUBLIC'")) {
            assertTrue(rs.next());
            assertEquals(8, rs.getInt(1));
        }
    }

    @Test
    public void testAlterAddAndDropExternalizedColumnLifecycle() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(64))"
                + " DBPARTITION BY HASH(id) TBPARTITION BY HASH(id) TBPARTITIONS 2");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES (1, 'before add')");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE_NAME + " ADD COLUMN content LONGTEXT EXTERNALIZE");
        assertMappingCount("PUBLIC", 1);
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT content FROM " + TABLE_NAME + " WHERE id=1")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE " + TABLE_NAME + " SET content='after add' WHERE id=1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE_NAME + " DROP COLUMN content");
        assertMappingCount("PUBLIC", 0);
        assertMappingCountAtLeast("DROP", 1);
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW CREATE TABLE " + TABLE_NAME)) {
            assertTrue(rs.next());
            assertFalse(rs.getString(2).toLowerCase(Locale.ROOT).contains("`content`"));
        }
    }

    @Test
    public void testSpecialColumnNameRoundTrip() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "`my content` LONGTEXT EXTERNALIZE"
                + ") DBPARTITION BY HASH(id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " (id, `my content`) VALUES (1, 'special name')");
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT `my content` FROM " + TABLE_NAME + " WHERE id=1")) {
            assertTrue(rs.next());
            assertEquals("special name", rs.getString(1));
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW CREATE TABLE " + TABLE_NAME)) {
            assertTrue(rs.next());
            String createSql = rs.getString(2).toLowerCase(Locale.ROOT);
            assertTrue(createSql.contains("`my content` longtext externalize"));
            assertFalse(createSql.contains("my content_addr_"));
        }
    }

    @Test
    public void testExternalizedColumnDdlRestrictions() {
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " (id BIGINT PRIMARY KEY, content INT EXTERNALIZE)"
                + " DBPARTITION BY HASH(id)",
            "does not support EXTERNALIZE");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT PRIMARY KEY, content LONGTEXT DEFAULT 'x' EXTERNALIZE)"
                + " DBPARTITION BY HASH(id)",
            "DEFAULT");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT PRIMARY KEY, name VARCHAR(64), content LONGTEXT EXTERNALIZE)"
                + " DBPARTITION BY HASH(id)");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "ALTER TABLE " + TABLE_NAME + " ADD INDEX idx_content(content)",
            "Cannot create index on externalized column");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "TRUNCATE TABLE " + TABLE_NAME,
            "with externalized columns");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "RENAME TABLE " + TABLE_NAME + " TO " + TABLE_NAME + "_renamed",
            "is not supported on table");
    }

    @Test
    public void testMceWithCompositePrimaryKey() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT NOT NULL, sub_id BIGINT NOT NULL, content LONGTEXT,"
                + "PRIMARY KEY(id, sub_id))"
                + " DBPARTITION BY HASH(id) TBPARTITION BY HASH(sub_id) TBPARTITIONS 2");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + TABLE_NAME + " VALUES"
                + " (1, 1, 'composite one'), (1, 2, 'composite two'), (2, 1, NULL)");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + TABLE_NAME + " MODIFY COLUMN content LONGTEXT EXTERNALIZE");
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT id, sub_id, content FROM " + TABLE_NAME + " ORDER BY id, sub_id")) {
            assertTrue(rs.next());
            assertEquals("composite one", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("composite two", rs.getString(3));
            assertTrue(rs.next());
            assertNull(rs.getString(3));
            assertFalse(rs.next());
        }
    }

    private void assertMappingCount(String status, int expected) throws SQLException {
        assertMappingCount(status, expected, false);
    }

    private void assertMappingCountAtLeast(String status, int expected) throws SQLException {
        assertMappingCount(status, expected, true);
    }

    private void assertMappingCount(String status, int expected, boolean atLeast) throws SQLException {
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "SELECT COUNT(*) FROM metadb.ext_column_mapping"
                + " WHERE table_schema = ? AND table_name = ? AND column_name = 'content' AND status = ?")) {
            ps.setString(1, CLASS_DB);
            ps.setString(2, TABLE_NAME);
            ps.setString(3, status);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                if (atLeast) {
                    assertTrue(rs.getInt(1) >= expected);
                } else {
                    assertEquals(expected, rs.getInt(1));
                }
            }
        }
    }

    private static String randomSuffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

}
