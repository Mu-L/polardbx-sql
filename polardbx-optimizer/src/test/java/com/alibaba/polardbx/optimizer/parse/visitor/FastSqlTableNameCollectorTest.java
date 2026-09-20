package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLConstraintImpl;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import org.apache.calcite.sql.SqlIdentifier;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author mengshi
 */
public class FastSqlTableNameCollectorTest {

    @Test
    public void test1() {
        String sql = "delete from d3.t1";
        Assert.assertEquals("[(d3, t1)]", getTables(sql));
    }

    @Test
    public void test2() {
        String sql = "delete from t1";
        Assert.assertEquals("[(null, t1)]", getTables(sql));
    }

    @Test
    public void test3() {
        String sql = "delete from t1 where id = (select * from d2.t2)";
        Assert.assertEquals("[(d2, t2), (null, t1)]", getTables(sql));
    }

    @Test
    public void test4() {
        String sql =
            "delete ignore a.*, b.* from `drds_polarx2_qatest_app`.update_delete_base_broadcast a, `drds_polarx2_qatest_app`.update_delete_base_two_one_db_one_tb b where a.pk = b.pk";
        Assert.assertEquals(
            "[(drds_polarx2_qatest_app, update_delete_base_two_one_db_one_tb), (drds_polarx2_qatest_app, update_delete_base_broadcast)]",
            getTables(sql));
    }

    @Test
    public void test5() {
        String sql =
            "select a as a from `db1`.`t` as tt,db2.t2,db3.`t3`,t4, (select * from t5) where id = (select * from d6.t6)";
        Assert.assertEquals("[(db3, t3), (null, t4), (db1, t), (null, t5), (d6, t6), (db2, t2)]", getTables(sql));
    }

    @Test
    public void test6() {
        String sql = "update t1 set a=1";
        Assert.assertEquals("[(null, t1)]", getTables(sql));
    }

    @Test
    public void test7() {
        String sql = "UPDATE student s JOIN class c ON s.class_id = c.id SET s.class_name='test11',c.stu_name='test11'";
        Assert.assertEquals("[(null, student), (null, class)]", getTables(sql));
    }

    @Test
    public void test8() {
        String sql =
            "delete from `drds_polarx2_qatest_app`.update_delete_base_multi_db_multi_tb.*, `drds_polarx2_qatest_app`.update_delete_base_two_multi_db_multi_tb.*, a.* using `drds_polarx2_qatest_app`.update_delete_base_multi_db_multi_tb, `drds_polarx2_qatest_app`.update_delete_base_two_multi_db_multi_tb, `drds_polarx2_qatest_app`.update_delete_base_three_broadcast as a ";
        Assert.assertEquals(
            "[(drds_polarx2_qatest_app, update_delete_base_two_multi_db_multi_tb), (drds_polarx2_qatest_app, update_delete_base_three_broadcast), (drds_polarx2_qatest_app, update_delete_base_multi_db_multi_tb)]",
            getTables(sql));
    }

    @Test
    public void testExternalCatalogThreePartTableCollectsEncodedSchema() {
        try (MockedStatic<ExternalCatalogManager> mocked = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager manager = mock(ExternalCatalogManager.class);
            mocked.when(ExternalCatalogManager::getInstance).thenReturn(manager);
            when(manager.exists("spm_test_cat")).thenReturn(true);

            String sql = "select * from spm_test_cat.spm_test_db.spm_test_t";

            Assert.assertEquals("[(spm_test_cat$$spm_test_db, spm_test_t)]", getTables(sql));
        }
    }

    @Test
    public void testNonExternalThreePartTableKeepsFastSqlSchemaAndTable() {
        try (MockedStatic<ExternalCatalogManager> mocked = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager manager = mock(ExternalCatalogManager.class);
            mocked.when(ExternalCatalogManager::getInstance).thenReturn(manager);
            when(manager.exists("normal_cat")).thenReturn(false);

            String sql = "select * from normal_cat.normal_db.normal_t";

            Assert.assertEquals("[(normal_db, normal_t)]", getTables(sql));
        }
    }

    @Test
    public void testSqlParameterizedTablesEncodeExternalThreePartTable() {
        try (MockedStatic<ExternalCatalogManager> mocked = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager manager = mock(ExternalCatalogManager.class);
            mocked.when(ExternalCatalogManager::getInstance).thenReturn(manager);
            when(manager.exists("spm_test_cat")).thenReturn(true);

            SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
                "SELECT * FROM spm_test_cat.spm_test_db.spm_test_t");

            Assert.assertEquals("SELECT *\nFROM spm_test_cat.spm_test_db.spm_test_t", parameterized.getSql());
            Assert.assertEquals("[(spm_test_cat$$spm_test_db, spm_test_t)]", parameterized.getTables().toString());
        }
    }

    public String getTables(String sql) {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        FastSqlTableNameCollector visitor = new FastSqlTableNameCollector();
        stmt.accept(visitor);
        System.out.println(visitor.getTables());
        return visitor.getTables().toString();
    }

    public boolean referencesExternalTable(String sql) {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        FastSqlTableNameCollector visitor = new FastSqlTableNameCollector();
        stmt.accept(visitor);
        return visitor.isReferencesExternalTable();
    }

    public SqlParameterized parameterize(String sql) {
        return SqlParameterizeUtils.parameterize(sql);
    }

    @Test
    public void testReferencesExternalTableIsFalseForNormalTable() {
        Assert.assertFalse(referencesExternalTable("select * from normal_db.normal_t"));
    }

    @Test
    public void testReferencesExternalTableIsTrueForExternalSchemaTable() {
        try (MockedStatic<ExternalCatalogManager> mocked = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager manager = mock(ExternalCatalogManager.class);
            mocked.when(ExternalCatalogManager::getInstance).thenReturn(manager);
            when(manager.exists("spm_test_cat")).thenReturn(true);

            Assert.assertTrue(referencesExternalTable("select * from spm_test_cat$$spm_test_db.spm_test_t"));
        }
    }

    @Test
    public void testReferencesExternalTableIsTrueForExternalCatalogThreePartTable() {
        try (MockedStatic<ExternalCatalogManager> mocked = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager manager = mock(ExternalCatalogManager.class);
            mocked.when(ExternalCatalogManager::getInstance).thenReturn(manager);
            when(manager.exists("spm_test_cat")).thenReturn(true);

            Assert.assertTrue(referencesExternalTable("select * from spm_test_cat.spm_test_db.spm_test_t"));
        }
    }

    @Test
    public void testReferencesExternalTableIsTrueForFilesTableSource() {
        Assert.assertTrue(referencesExternalTable(
            "select * from files('connector'='mock', 'mock.columns'='id:int') f"));
    }

    @Test
    public void testReferencesExternalTableIsTrueForNativeQueryTableSource() {
        try (MockedStatic<ExternalCatalogManager> mocked = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager manager = mock(ExternalCatalogManager.class);
            mocked.when(ExternalCatalogManager::getInstance).thenReturn(manager);
            when(manager.exists("spm_test_cat")).thenReturn(true);

            Assert.assertTrue(referencesExternalTable(
                "select * from (spm_test_cat.native_query('select * from t')) nq"));
        }
    }

    @Test
    public void testSqlParameterizedStoresExternalReferenceFlagForExternalTable() {
        try (MockedStatic<ExternalCatalogManager> mocked = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager manager = mock(ExternalCatalogManager.class);
            mocked.when(ExternalCatalogManager::getInstance).thenReturn(manager);
            when(manager.exists("spm_test_cat")).thenReturn(true);

            SqlParameterized parameterized = parameterize("select * from spm_test_cat.spm_test_db.spm_test_t");

            Assert.assertTrue(parameterized.isReferencesExternalTable());
        }
    }

    @Test
    public void testSqlParameterizedStoresExternalReferenceFlagForNormalTable() {
        SqlParameterized parameterized = parameterize("select * from normal_db.normal_t");

        Assert.assertFalse(parameterized.isReferencesExternalTable());
    }

    @Test
    public void testAssignConstraintNameWithBacktick() {
        FastSqlToCalciteNodeVisitor visitor = mock(FastSqlToCalciteNodeVisitor.class);
        SQLConstraintImpl constraint = mock(MySqlKey.class);
        SqlIdentifier tableName = mock(SqlIdentifier.class);
        SQLSelectOrderByItem column = mock(SQLSelectOrderByItem.class);
        SQLExpr expr = mock(SQLExpr.class);

        String columnName = "` special columnName`";

        try (MockedStatic<SQLUtils> utilities = mockStatic(SQLUtils.class)) {
            when(expr.toString()).thenReturn(columnName);
            when(column.getExpr()).thenReturn(expr);
            when(((MySqlKey) constraint).getColumns()).thenReturn(Collections.singletonList(column));
            utilities.when(() -> SQLUtils.normalizeNoTrim(columnName))
                .thenReturn(" special columnName"); // Will remove backticks
            Set<String> indexNamesSet = new HashSet<>();

            doCallRealMethod().when(visitor).assignConstraintName(constraint, indexNamesSet, tableName);
            visitor.assignConstraintName(constraint, indexNamesSet, tableName);

            verify(constraint).setName("` special columnName`");
            assertTrue(indexNamesSet.contains(" special columnName"));
        }
    }

    @Test
    public void testAssignConstraintNameWithBacktickAndIncrement() {
        FastSqlToCalciteNodeVisitor visitor = mock(FastSqlToCalciteNodeVisitor.class);
        SQLConstraintImpl constraint = mock(MySqlKey.class);
        SqlIdentifier tableName = mock(SqlIdentifier.class);
        SQLSelectOrderByItem column = mock(SQLSelectOrderByItem.class);
        SQLExpr expr = mock(SQLExpr.class);

        String baseName = "`columnName`";
        String normalizedName = "columnName";

        try (MockedStatic<SQLUtils> utilities = mockStatic(SQLUtils.class)) {
            when(expr.toString()).thenReturn(baseName);
            when(column.getExpr()).thenReturn(expr);
            when(((MySqlKey) constraint).getColumns()).thenReturn(Collections.singletonList(column));
            utilities.when(() -> SQLUtils.normalizeNoTrim(baseName))
                .thenReturn(normalizedName); // Will remove backticks
            Set<String> indexNamesSet = new HashSet<>();
            indexNamesSet.add(normalizedName); // Already contains the normalized name without backticks

            doCallRealMethod().when(visitor).assignConstraintName(constraint, indexNamesSet, tableName);
            visitor.assignConstraintName(constraint, indexNamesSet, tableName);

            String expectedName = "`columnName_2`";
            verify(constraint).setName(expectedName);
            assertTrue(indexNamesSet.contains("columnName_2"));
        }
    }

    // Tests for cases where index names are already taken, but without backticks
    @Test
    public void testAssignConstraintNameWhenNameIsTakenWithoutBacktick() {
        FastSqlToCalciteNodeVisitor visitor = mock(FastSqlToCalciteNodeVisitor.class);
        SQLConstraintImpl constraint = mock(MySqlKey.class);
        SqlIdentifier tableName = mock(SqlIdentifier.class);
        SQLSelectOrderByItem column = mock(SQLSelectOrderByItem.class);
        SQLExpr expr = mock(SQLExpr.class);
        String baseName = "columnName";

        try (MockedStatic<SQLUtils> utilities = mockStatic(SQLUtils.class)) {
            when(expr.toString()).thenReturn(baseName);
            when(column.getExpr()).thenReturn(expr);
            when(((MySqlKey) constraint).getColumns()).thenReturn(Collections.singletonList(column));
            utilities.when(() -> SQLUtils.normalizeNoTrim(baseName)).thenReturn(baseName);
            Set<String> indexNamesSet = new HashSet<>();
            indexNamesSet.add(baseName);
            indexNamesSet.add(baseName + "_2"); // Name is already taken

            String expectedNameNormalized = "columnName_";
            int expectedProb = 2;
            while (indexNamesSet.contains(expectedNameNormalized + expectedProb)) {
                ++expectedProb;
            }
            String expectedName = "`columnName_" + expectedProb + "`";

            doCallRealMethod().when(visitor).assignConstraintName(constraint, indexNamesSet, tableName);
            visitor.assignConstraintName(constraint, indexNamesSet, tableName);

            verify(constraint).setName(expectedName);
            assertTrue(indexNamesSet.contains("columnName_" + expectedProb));
        }
    }

    @Test
    public void testAssignConstraintNameForMysqlForeignKeyWhenNameIsNotTaken() {
        FastSqlToCalciteNodeVisitor visitor = mock(FastSqlToCalciteNodeVisitor.class);
        SQLConstraintImpl constraint = mock(MysqlForeignKey.class);
        SqlIdentifier tableName = mock(SqlIdentifier.class);
        when(tableName.getLastName()).thenReturn("mytable");

        try (MockedStatic<SQLUtils> utilities = mockStatic(SQLUtils.class)) {
            String tableNameNormalized = tableName.getLastName().toLowerCase();
            utilities.when(() -> SQLUtils.normalizeNoTrim(tableNameNormalized)).thenReturn(tableNameNormalized);

            Set<String> indexNamesSet = new HashSet<>();

            doCallRealMethod().when(visitor).assignConstraintName(constraint, indexNamesSet, tableName);
            visitor.assignConstraintName(constraint, indexNamesSet, tableName);

            String expectedBaseName = tableNameNormalized + "_ibfk_1";
            String expectedName = "`" + tableNameNormalized + "_ibfk_1`";

            verify(constraint).setName(expectedName);
            assertTrue(indexNamesSet.contains(expectedBaseName));
        }
    }

    @Test
    public void testAssignConstraintNameForMysqlForeignKeyWhenNameIsTaken() {
        FastSqlToCalciteNodeVisitor visitor = mock(FastSqlToCalciteNodeVisitor.class);
        SQLConstraintImpl constraint = mock(MysqlForeignKey.class);
        SqlIdentifier tableName = mock(SqlIdentifier.class);
        when(tableName.getLastName()).thenReturn("mytable");

        try (MockedStatic<SQLUtils> utilities = mockStatic(SQLUtils.class)) {
            String tableNameNormalized = tableName.getLastName().toLowerCase();
            utilities.when(() -> SQLUtils.normalizeNoTrim(tableNameNormalized)).thenReturn(tableNameNormalized);

            Set<String> indexNamesSet = new HashSet<>();
            indexNamesSet.add(tableNameNormalized + "_ibfk_1"); // Simulate that the base name is already taken

            doCallRealMethod().when(visitor).assignConstraintName(constraint, indexNamesSet, tableName);
            visitor.assignConstraintName(constraint, indexNamesSet, tableName);

            String expectedBaseName = tableNameNormalized + "_ibfk_2";
            String expectedName = "`" + tableNameNormalized + "_ibfk_2`";

            verify(constraint).setName(expectedName);
            assertTrue(indexNamesSet.contains(expectedBaseName));
        }
    }

    @Test
    public void testQuotedExternalThreePartTableIsCollected() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("hive", "mock", new HashMap<>(), null, null));
        try {
            Assert.assertEquals("[(hive$$dwd, t1)]", getTables("select * from `hive`.`dwd`.`t1`"));
        } finally {
            ExternalCatalogManager.getInstance().remove("hive");
        }
    }
}
