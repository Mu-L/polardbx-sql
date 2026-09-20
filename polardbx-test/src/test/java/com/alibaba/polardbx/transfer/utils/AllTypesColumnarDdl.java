package com.alibaba.polardbx.transfer.utils;

import com.alibaba.polardbx.transfer.plugin.AllTypesWriteOnlyPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class AllTypesColumnarDdl {
    private static final Logger logger = LoggerFactory.getLogger(AllTypesColumnarDdl.class);

    public static List<String> check(Statement stmt, String cciName) throws SQLException {
        AllTypesWriteOnlyPlugin.getLock().writeLock().lock();
        try {
            String tableName = "all_types";
            List<String> checkResults = new ArrayList<>();
//            String checkSql = "CHECK COLUMNAR INDEX " + cciName;
//            ResultSet rs = stmt.executeQuery(checkSql);
//            while (rs.next()) {
//                checkResults.add(rs.getString("DETAILS"));
//            }
            ResultSet rs = stmt.executeQuery("call polardbx.columnar_flush()");
            long tso = 0;
            if (rs.next()) {
                tso = rs.getLong(1);
            } else {
                throw new RuntimeException("call columnar flush empty results");
            }
            // wait for sync
            while (true) {
                rs = stmt.executeQuery("show columnar offset");
                boolean ok = false;
                while (rs.next()) {
                    String type = rs.getString("TYPE");
                    if ("CN_MIN_LATENCY".equalsIgnoreCase(type)) {
                        if (rs.getLong("TSO") >= tso) {
                            ok = true;
                        }
                        break;
                    }
                }
                if (ok) {
                    break;
                } else {
                    Thread.sleep(1000);
                }
            }
            // column store
            String sql = "/*+TDDL:a()*/ select check_sum_v2(*) from " + tableName
                + " as of tso " + tso + " force index (" + cciName + ")";
            rs = stmt.executeQuery(sql);
            long columnChecksum = -1;
            if (rs.next()) {
                columnChecksum = rs.getLong(1);
            }
            // row store
            rs = stmt.executeQuery("/*+TDDL:a()*/ select check_sum_v2(*) from " + tableName
                + " as of tso " + tso + " force index (primary)");
            long rowChecksum = -1;
            if (rs.next()) {
                rowChecksum = rs.getLong(1);
            }
            if (columnChecksum != rowChecksum) {
                checkResults.add(
                    "cciName: " + cciName + " tso " + tso + ", column checksum " + columnChecksum + ", row checksum "
                        + rowChecksum);
            } else {
                checkResults.add("OK");
            }
            return checkResults;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            AllTypesWriteOnlyPlugin.getLock().writeLock().unlock();
        }
    }

    public static void createCci(Statement stmt, String cciName, String sortKey, String partitionDef, String type)
        throws SQLException {
        String sql = "CREATE CLUSTERED COLUMNAR INDEX "
            + cciName + " ON all_types(" + sortKey + ") " + partitionDef;
        if (null != type) {
            sql += "columnar_options='{\"type\":\"" + type + "\"}'";
        }
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterTableAddCci(Statement stmt, String cciName, String sortKey, String partitionDef,
                                        String type) throws SQLException {
        String sql = "ALTER TABLE all_types ADD CLUSTERED COLUMNAR INDEX "
            + cciName + "(" + sortKey + ") " + partitionDef;
        if (null != type) {
            sql += "columnar_options='{\"type\":\"" + type + "\"}'";
        }
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void dropCci(Statement stmt, String cciName) throws SQLException {
        String sql = "DROP INDEX " + cciName + " ON all_types";
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterTableDropCci(Statement stmt, String cciName) throws SQLException {
        String sql = "ALTER TABLE all_types DROP INDEX " + cciName;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void truncateTable(Statement stmt) throws SQLException {
        String sql = "/*+TDDL:FORBID_TRUNCATE_WITH_ARCHIVE_CCI=false*/ TRUNCATE TABLE all_types";
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void createTableLike(Statement stmt, String tableName) throws SQLException {
        String sql = "CREATE TABLE " + tableName + " LIKE all_types";
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void dropTable(Statement stmt, String tableName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void renameTable(Statement stmt, String tmpTableName, String newTableName) throws SQLException {
        String sql = "RENAME TABLE all_types TO " + tmpTableName + "," + newTableName + " TO all_types";
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterTableRenameTable(Statement stmt, String tmpTableName, String newTableName)
        throws SQLException {
        String sql = "ALTER TABLE all_types RENAME TO " + tmpTableName;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
        sql = "ALTER TABLE " + newTableName + " RENAME TO all_types";
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterTableAddColumn(Statement stmt, String columnName, String type) throws SQLException {
        String sql = "ALTER TABLE all_types ADD COLUMN " + columnName + " " + type;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterTableDropColumn(Statement stmt, String columnName) throws SQLException {
        String sql = "ALTER TABLE all_types DROP COLUMN " + columnName;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterTableModifyColumn(Statement stmt, String columnName, String newType, boolean omc)
        throws SQLException {
        String sql = "/*+TDDL:ENABLE_MODIFY_CCI_CRITICAL_COLUMN=true REBUILD_CCI_STRATEGY=0 */ "
            + "ALTER TABLE all_types MODIFY COLUMN " + columnName + " " + newType;
        if (omc) {
            sql =
                "/*+TDDL:ENABLE_MODIFY_CCI_CRITICAL_COLUMN=true REBUILD_CCI_STRATEGY=0 OMC_FORCE_TYPE_CONVERSION=true*/ "
                    + "ALTER TABLE all_types MODIFY COLUMN " + columnName + " " + newType + ", ALGORITHM = omc";
        }
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterTableChangeColumn(Statement stmt, String oldColumnName, String newColumnName,
                                              String newType) throws SQLException {
        String sql = "ALTER TABLE all_types CHANGE COLUMN " + oldColumnName + " " + newColumnName + " " + newType;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterTableSetDefault(Statement stmt, String columnName, String defaultValue)
        throws SQLException {
        String sql = "ALTER TABLE all_types ALTER COLUMN " + columnName + " SET DEFAULT " + defaultValue;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterTableDropDefault(Statement stmt, String columnName) throws SQLException {
        String sql = "ALTER TABLE all_types ALTER COLUMN " + columnName + " DROP DEFAULT";
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void multiAlter(Statement stmt, String modifyColumnName, String modifyType,
                                  String oldColumnName, String newColumnName, String newType) throws SQLException {
        String sql = "ALTER TABLE all_types MODIFY COLUMN " + modifyColumnName + " " + modifyType + ", " +
            "CHANGE COLUMN " + oldColumnName + " " + newColumnName + " " + newType;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void renameCci(Statement stmt, String tableName, String cciName, String newCciName)
        throws SQLException {
        String sql = "ALTER TABLE " + tableName + " RENAME INDEX " + cciName + " TO " + newCciName;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterPrimaryPartition(Statement stmt, String partitionDef) throws SQLException {
        String sql = "ALTER TABLE all_types " + partitionDef;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterPrimaryDropPartition(Statement stmt, String partition) throws SQLException {
        String sql = "/*+TDDL:ENABLE_DROP_TRUNCATE_CCI_PARTITION=true ENABLE_SHADOW_INSERT_ON_DROP_PARTITION=TRUE*/ "
            + "ALTER TABLE all_types DROP PARTITION " + partition;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterCciAddPartition(Statement stmt, String cciName, String partitionDef) throws SQLException {
        String sql = "ALTER TABLE all_types." + cciName + " ADD PARTITION " + partitionDef;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }

    public static void alterCciSplitPartition(Statement stmt, String cciName, String oldPartition, String partitionDef)
        throws SQLException {
        String sql = "ALTER TABLE all_types." + cciName + " SPLIT PARTITION " + oldPartition + " INTO " + partitionDef;
        logger.info("execute ddl: {}", sql);
        stmt.execute(sql);
    }
}
