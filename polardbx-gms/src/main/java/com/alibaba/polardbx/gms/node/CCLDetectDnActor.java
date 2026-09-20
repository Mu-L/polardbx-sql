package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.ccl.DnCclRecord;
import com.google.common.collect.ImmutableSet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.node.CCLDetectUtils.DubiousItem;

/**
 * @author liugaoji
 */
public class CCLDetectDnActor {
    private static final Logger logger = LoggerFactory.getLogger(CCLDetectDnActor.class);

    enum DNVersion {
        UNCHKED, NEW_VERSION, OLD_VERSION
    }

    private volatile DNVersion isNewVersion = DNVersion.UNCHKED;
    private final Object versionCheckLock = new Object();

    private static final String ACTIVE_SESSION_NUM =
        "select count(*) active_session from information_schema.processlist where command <> 'Sleep' and command <> 'Binlog Dump' and state <> 'Concurrency control waiting' ";

    private static final String ACTIVE_SESSION =
        "select ID, INFO, TIME from information_schema.processlist where INFO is not NULL and command <> 'Sleep' and command <> 'Binlog Dump' and state <> 'Concurrency control waiting'  and INFO like '%SELECT%'";

    private static final String ACTIVE_SESSION_NUM_WITH_SELECT =
        ACTIVE_SESSION_NUM + " and INFO like '%SELECT%'";
    private static final String WAITING_LIST =
        "select ID, INFO from information_schema.processlist where INFO is not NULL and command <> 'Sleep' and command <> 'Binlog Dump' and state = 'Concurrency control waiting'";

    private static final String ACTIVE_SESSION_NUM_NEW =
        "select count(*) active_session from information_schema.milli_processlist where command <> 'Sleep' and command <> 'Binlog Dump' and state <> 'Concurrency control waiting' ";

    private static final String ACTIVE_SESSION_NEW =
        "select ID, INFO, TIME from information_schema.milli_processlist where INFO is not NULL and command <> 'Sleep' and command <> 'Binlog Dump' and state <> 'Concurrency control waiting'  and INFO like '%SELECT%'";

    private static final String ACTIVE_SESSION_NUM_WITH_SELECT_NEW =
        ACTIVE_SESSION_NUM_NEW + " and INFO like '%SELECT%'";
    private static final String WAITING_LIST_NEW =
        "select ID, INFO from information_schema.milli_processlist where INFO is not NULL and command <> 'Sleep' and command <> 'Binlog Dump' and state = 'Concurrency control waiting'";

    // use this in default
    // dbms_ccl.add_ccl_rule('<Type>','<Schema_name>','<Table_name>',<Concurrency_count>,'<Keywords>');
    public static final String CREATE_DN_CCL_RULE =
        "call dbms_ccl.add_ccl_rule('SELECT', '', '', %s, '%s')";

    private static final String CREATE_DN_CCL_RULE_NEW =
        "call dbms_ccl.add_ccl_rule_returning('SELECT', '', '', %s, '%s')";

    private static final String DN_VERSION_TEST =
        "select count(*) from information_schema.milli_processlist";

    private static final String KILL_DN_SESSION =
        "kill %s";

    private static final String SHOW_DN_CCL_RULE =
        "call dbms_ccl.show_ccl_rule()";

    private static final String DELETE_DN_CCL_RULE =
        "call dbms_ccl.del_ccl_rule(%s)";

    // call dbms_ccl.del_ccl_rule_batch('1,2,3')
    private static final String DELETE_DN_CCL_RULE_BATCH =
        "call dbms_ccl.del_ccl_rule_batch('%s')";

    private static final String DELETE_ALL_DN_CCL_RULE =
        "call dbms_ccl.del_all_ccl_rule()";

    private static final Set<String> DDL_WAIT_MDL_LOCK_TYPE =
        ImmutableSet.of(
            "EXCLUSIVE", // for general online ddl: add column, drop column
            "SHARED_NO_READ_WRITE", // for optimize table, add index.
            "SHARED_NO_WRITE" // for modify column, modify partition.
        );

    private static final String DDL_WAIT_MDL_LOCK_TYPE_STRING =
        DDL_WAIT_MDL_LOCK_TYPE.stream()
            .map(s -> "'" + s + "'")
            .collect(Collectors.joining(","));

    private static final String SELECT_MDL_WAITING_ONLY =
        "select "
            + "`g`.`OBJECT_SCHEMA` AS `object_schema`,"
            + "`g`.`OBJECT_NAME` AS `object_name`,"
            + "`g`.`OBJECT_TYPE` AS `object_type`,"
            + "`g`.`LOCK_TYPE` AS `lock_type`,"
            + "`g`.`LOCK_STATUS` AS `lock_status`,"
            + "`g`.`OWNER_THREAD_ID` AS `owner_thread_id` "
            + "from `performance_schema`.`metadata_locks` `g` "
            + "where `lock_type` in ("
            + DDL_WAIT_MDL_LOCK_TYPE_STRING
            + ")";

    public String getGenSql(Supplier<Connection> connectionSupplier, long concurrency, String key) {
        makeDnVersionChecked(connectionSupplier);
        return isNewVersion == DNVersion.NEW_VERSION ? String.format(CREATE_DN_CCL_RULE_NEW, concurrency, key) :
            String.format(CREATE_DN_CCL_RULE, concurrency, key);
    }

    public Long killAndGenerateCcl(Supplier<Connection> connectionSupplier, List<DubiousItem> toKill,
                                   boolean shouldGen,
                                   final String genSql) throws SQLException {
        Connection leaderConn = null;
        Statement stmt = null;
        Long id = -1L;
        try {
            leaderConn = connectionSupplier.get();
            if (leaderConn != null) {
                ensureDnVersionChecked(leaderConn);
                stmt = leaderConn.createStatement();
                for (DubiousItem statement : toKill) {
                    String killStmt = String.format(KILL_DN_SESSION, statement.id);
                    stmt.execute(killStmt);
                }
                if (shouldGen) {
                    if (stmt != null) {
                        stmt.execute(genSql);
                        if (isNewVersion == DNVersion.NEW_VERSION) {
                            ResultSet resultSet = stmt.getResultSet();
                            if (resultSet != null && resultSet.next()) {
                                id = resultSet.getLong("RULE_ID");
                            }
                        }
                    }
                    return id;
                }
            }
        } catch (Exception e) {
            logger.error("CCL_DETECT generate DN CCL RULE error" + e.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.warn("CCL_DETECT close DN statement error" + e.getMessage());
                }
            }
            closeQuietly(leaderConn);
        }
        return id;
    }

    public Long generateCcl(Supplier<Connection> connectionSupplier, final String genSql) throws SQLException {
        Connection leaderConn = null;
        Statement stmt = null;
        Long id = -1L;
        try {
            leaderConn = connectionSupplier.get();
            if (leaderConn != null) {
                ensureDnVersionChecked(leaderConn);
                stmt = leaderConn.createStatement();
                stmt.execute(genSql);
                if (isNewVersion == DNVersion.NEW_VERSION) {
                    ResultSet resultSet = stmt.getResultSet();
                    if (resultSet != null && resultSet.next()) {
                        id = resultSet.getLong("RULE_ID");
                    }
                }
            }
        } catch (Exception e) {
            logger.error("CCL_DETECT generate DN CCL RULE error: " + e.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.warn("CCL_DETECT close DN statement error" + e.getMessage());
                }
            }
            closeQuietly(leaderConn);
        }
        return id;
    }

    // kill connection waiting on CCL before add new ccl
    public void deleteDnCCLWithKill(Supplier<Connection> connectionSupplier, Pair<String, Long> cclRuleKey) {
        Connection leaderConn = null;
        try {
            leaderConn = connectionSupplier.get();
            Statement stmt = null;
            if (leaderConn != null) {
                ensureDnVersionChecked(leaderConn);
                stmt = leaderConn.createStatement();

                stmt.execute(isNewVersion == DNVersion.NEW_VERSION ? WAITING_LIST_NEW : WAITING_LIST);
                ResultSet result = stmt.getResultSet();
                List<Long> ids = new ArrayList<>();
                while (result.next()) {
                    if (result.getString("INFO").contains(cclRuleKey.getKey())) {
                        ids.add(result.getLong(1));
                    }
                }
                logger.warn("CCL_DETECT DELETE DN CCL RULE " + cclRuleKey.getKey() + " IDs " + ids.size());
                for (Long id : ids) {
                    stmt.execute(String.format(KILL_DN_SESSION, id));
                }

                if (isNewVersion == DNVersion.NEW_VERSION) {
                    String id = cclRuleKey.getValue().toString();
                    stmt.execute(String.format(DELETE_DN_CCL_RULE, id));
                } else {
                    stmt.execute(String.format(SHOW_DN_CCL_RULE));
                    ResultSet set = stmt.getResultSet();

                    // id, concurrency
                    // only keep the element with minimal concurrency
                    List<Pair<Long, Long>> ruleIds = new ArrayList<>();
                    while (set.next()) {
                        logger.warn(
                            String.format("CCL_DETECT DELETE DN CCL RULE %s KEYWORDS %s ID %s CONCURRENCY_COUNT %s",
                                cclRuleKey.getKey(),
                                set.getString("KEYWORDS"), set.getString("ID"), set.getString("CONCURRENCY_COUNT")));
                        if (cclRuleKey.getKey().equals(set.getString("KEYWORDS"))) {
                            ruleIds.add(Pair.of(set.getLong("ID"), set.getLong("CONCURRENCY_COUNT")));
                        }
                    }
                    ruleIds.sort(Comparator.comparing(Pair::getValue));
                    for (int i = 1; i < ruleIds.size(); i++) {
                        stmt.execute(String.format(DELETE_DN_CCL_RULE, ruleIds.get(i).getKey()));
                    }
                    set.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Throwable e) {
            logger.error("CCL_DETECT DELETE DN CCL RULE error" + e.getMessage());
        } finally {
            closeQuietly(leaderConn);
        }
    }

    public void deleteDnCCL(Supplier<Connection> connectionSupplier, HashMap<String, Long> cclRuleKey) {
        Connection leaderConn = null;
        try {
            leaderConn = connectionSupplier.get();
            Statement stmt = null;
            if (leaderConn != null) {
                ensureDnVersionChecked(leaderConn);
                stmt = leaderConn.createStatement();
                if (isNewVersion == DNVersion.NEW_VERSION) {
                    if (!cclRuleKey.isEmpty()) {
                        String ids = cclRuleKey.values().stream().map(String::valueOf).collect(Collectors.joining(","));
                        stmt.execute(String.format(DELETE_DN_CCL_RULE_BATCH, ids));
                    }
                } else {
                    stmt.execute(String.format(SHOW_DN_CCL_RULE));
                    ResultSet result = stmt.getResultSet();
                    List<Long> ids = new ArrayList<>();
                    while (result.next()) {
                        if (cclRuleKey.containsKey(result.getString("KEYWORDS"))) {
                            ids.add(result.getLong("ID"));
                        }
                    }
                    for (Long id : ids) {
                        stmt.execute(String.format(DELETE_DN_CCL_RULE, id));
                    }
                    result.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Throwable e) {
            logger.error("CCL_DETECT DELETE DN CCL RULE error" + e.getMessage());
        } finally {
            closeQuietly(leaderConn);
        }
    }

    public static boolean deleteAllDnCCL(Supplier<Connection> connectionSupplier) {
        Connection leaderConn = null;
        try {
            leaderConn = connectionSupplier.get();
            Statement stmt = null;
            if (leaderConn != null) {
                boolean isNew = checkNewVersionOnConn(leaderConn);
                stmt = leaderConn.createStatement();
                if (isNew) {
                    stmt.execute(DELETE_ALL_DN_CCL_RULE);
                } else {
                    stmt.execute(String.format(SHOW_DN_CCL_RULE));
                    ResultSet result = stmt.getResultSet();
                    List<Long> ids = new ArrayList<>();
                    while (result.next()) {
                        ids.add(result.getLong("ID"));
                    }
                    result.close();

                    // 然后执行删除操作
                    for (Long id : ids) {
                        stmt.execute(String.format(DELETE_DN_CCL_RULE, id));
                    }
                }
                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Throwable e) {
            logger.error("CCL_DETECT DELETE ALL DN CCL RULE error " + e.getMessage());
            return false;
        } finally {
            closeQuietly(leaderConn);
        }
        return true;
    }

    public long getCclConcurrency(int sqlConcurrency, int depth) {
        // 这里和DN的同学确认了一下，新版本和旧版本的并发度都可以设置为0，区别在于旧版本不能显式的指定等待队列的大小，但这里无影响
        return Math.max(0, sqlConcurrency >> (depth + 1));
    }

    // milli_processlist (new DN) reports TIME in milliseconds; the legacy processlist reports TIME in seconds
    public boolean isTimeInMillis(Supplier<Connection> connectionSupplier) {
        makeDnVersionChecked(connectionSupplier);
        return isNewVersion == DNVersion.NEW_VERSION;
    }

    public long getActiveSessionNum(Supplier<Connection> connectionSupplier, String instId, String storageId) {
        long activeSession = 0;
        Connection leaderConn = null;
        try {
            leaderConn = connectionSupplier.get();
            Statement stmt = null;
            if (leaderConn == null) {
                return -1;
            }

            ensureDnVersionChecked(leaderConn);
            boolean isNew = isNewVersion == DNVersion.NEW_VERSION;

            try {
                stmt = leaderConn.createStatement();
                String sql = DynamicConfig.getInstance().getCclDetectLevel().contains("dml") ?
                    isNew ? ACTIVE_SESSION_NUM_NEW : ACTIVE_SESSION_NUM :
                    isNew ? ACTIVE_SESSION_NUM_WITH_SELECT_NEW : ACTIVE_SESSION_NUM_WITH_SELECT;
                stmt.execute(sql);
                ResultSet result = stmt.getResultSet();
                if (result.next()) {
                    activeSession += result.getLong(1);
                }
                return activeSession;
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Throwable e) {
            logger.warn("CCL_DETECT getActiveSessionNum error for " + storageId, e);
        } finally {
            closeQuietly(leaderConn);
        }
        return -1;
    }

    public List<DubiousItem> getActiveSession(Supplier<Connection> connectionSupplier,
                                              String storageId) {
        List<DubiousItem> activeSession = new ArrayList<>();

        Connection leaderConn = null;
        try {
            leaderConn = connectionSupplier.get();
            Statement stmt = null;
            if (leaderConn == null) {
                return null;
            }

            ensureDnVersionChecked(leaderConn);
            boolean isNew = isNewVersion == DNVersion.NEW_VERSION;

            try {
                stmt = leaderConn.createStatement();

                stmt.execute(isNew ? ACTIVE_SESSION_NEW : ACTIVE_SESSION);

                ResultSet result = stmt.getResultSet();
                // we fetch all session here and filter later
                while (result.next()) {
                    activeSession.add(
                        new DubiousItem(result.getLong(1), result.getString(2), result.getLong(3)));
                }
                return activeSession;
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Throwable e) {
            logger.warn("CCL_DETECT check slave status error for " + storageId, e);
        } finally {
            closeQuietly(leaderConn);
        }
        return null;
    }

    public boolean hasDdlChecker(Supplier<Connection> connectionSupplier, String storageId) {
        Connection leaderConn = null;
        boolean hasDDL = false;
        try {
            leaderConn = connectionSupplier.get();
            Statement stmt = null;
            if (leaderConn == null) {
                return false;
            }
            try {
                stmt = leaderConn.createStatement();
//                logger.warn(String.format("CCL_DETECT ddlChecker inst %s sql %s", storageId, SELECT_MDL_WAITING_ONLY));
                stmt.execute(SELECT_MDL_WAITING_ONLY);
                ResultSet result = stmt.getResultSet();
                while (result.next()) {
                    hasDDL = true;
                    String objectSchema = result.getString("object_schema");
                    String objectName = result.getString("object_name");
                    String objectType = result.getString("object_type");
                    String lockType = result.getString("lock_type");
                    String lockStatus = result.getString("lock_status");
                    long ownerThreadId = result.getLong("owner_thread_id");
                    logger.warn(String.format(
                        "CCL_DETECT ddlChecker inst %s objectSchema %s objectName %s objectType %s lockType %s lockStatus %s ownerThreadId %s",
                        storageId, objectSchema, objectName, objectType, lockType, lockStatus, ownerThreadId));
                    break;
                }
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (Throwable e) {
            logger.error("CCL_DETECT ddlChecker error" + e.getMessage());
        } finally {
            closeQuietly(leaderConn);
        }
        if (DynamicConfig.getInstance().getCclDetectLevel().contains("ddl") && hasDDL) {
            return false;
        }
        return true;
    }

    public static List<DnCclRecord> getDnCclRules(Supplier<Connection> connectionSupplier, String instId,
                                                  String storageId) {
        List<DnCclRecord> records = new ArrayList<>();
        Connection leaderConn = null;
        try {
            leaderConn = connectionSupplier.get();
            Statement stmt = null;
            if (leaderConn == null) {
                return null;
            }

            try {
                stmt = leaderConn.createStatement();
                stmt.execute(SHOW_DN_CCL_RULE);
                ResultSet result = stmt.getResultSet();
                while (result.next()) {
                    DnCclRecord record = new DnCclRecord().fill(result);
                    record.inst = instId;
                    record.storageId = storageId;
                    records.add(record);
                }
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Throwable e) {
            logger.warn("CCL_DETECT getActiveSessionNum error for " + storageId, e);
        } finally {
            closeQuietly(leaderConn);
        }
        return records;
    }

    private void makeDnVersionChecked(Supplier<Connection> connectionSupplier) {
        if (isNewVersion == DNVersion.UNCHKED) {
            synchronized (versionCheckLock) {
                if (isNewVersion == DNVersion.UNCHKED) {
                    isNewVersion = isNewVersion(connectionSupplier) ? DNVersion.NEW_VERSION : DNVersion.OLD_VERSION;
                }
            }
        }
    }

    private void ensureDnVersionChecked(Connection leaderConn) {
        if (isNewVersion == DNVersion.UNCHKED) {
            synchronized (versionCheckLock) {
                if (isNewVersion == DNVersion.UNCHKED) {
                    isNewVersion = checkNewVersionOnConn(leaderConn) ? DNVersion.NEW_VERSION : DNVersion.OLD_VERSION;
                }
            }
        }
    }

    private static boolean checkNewVersionOnConn(Connection leaderConn) {
        boolean isNew = false;
        Statement stmt = null;
        try {
            stmt = leaderConn.createStatement();
            stmt.execute(String.format(DN_VERSION_TEST));
            ResultSet result = stmt.getResultSet();
            isNew = result.next();
        } catch (Throwable e) {
            isNew = false;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.warn("CCL_DETECT close DN statement error" + e.getMessage());
                }
            }
        }
        logger.warn("CCL_DETECT CHECK DN VERSION" + isNew);
        return isNew;
    }

    // also used by ClearAllDNCclProcedure.java
    public static boolean isNewVersion(Supplier<Connection> connectionSupplier) {
        Connection leaderConn = null;
        boolean isNew = false;
        try {
            leaderConn = connectionSupplier.get();
            Statement stmt = null;
            if (leaderConn != null) {
                stmt = leaderConn.createStatement();
                stmt.execute(String.format(DN_VERSION_TEST));
                ResultSet result = stmt.getResultSet();
                if (result.next()) {
                    isNew = true;
                } else {
                    isNew = false;
                }
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (Throwable e) {
            isNew = false;
        } finally {
            closeQuietly(leaderConn);
        }
        logger.warn("CCL_DETECT CHECK DN VERSION" + isNew);
        return isNew;
    }

    private static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("CCL_DETECT close DN connection error" + e.getMessage());
            }
        }
    }
}