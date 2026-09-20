package com.alibaba.polardbx.qatest.gms.ha;

import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class StorageHaTest {
    static {
        try {
            Class.forName(Druid.MYSQL_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    protected static final Log log = LogFactory.getLog(StorageHaTest.class);

    protected final static String selectDnGlobalPaxosInfoStmt =
        "select * from information_schema.alisql_cluster_global";
    protected final static String nodeHintTemp = " /*+TDDL:node=%s*/";
    //    protected final static String changeLeaderStmtTemp = "call dbms_consensus.change_leader(\'%s\')";
    protected final static String changeLeaderStmtTemp =
        " /*+TDDL:cmd_extra(FORCE_CHANGE_ROLE=true)*/ALTER SYSTEM /* change role for node=%s */ CHANGE_ROLE NODE '%s' TO LEADER;";
    protected final static String showDataSources = "show datasources where `WRITE_WEIGHT`>0 and `GROUP`!='MetaDB'";
    protected final static String HA_DB_NAME = "hadb";
    protected final static String HA_TB_NAME = "hatb";
    protected final static String HA_TB_DML_TEMP = "update `%s`.`%s` set b=b+1 where a > 0";

    private static String buildSelectDnGlobalPaxosInfoStmtByNodeNum(int nodeNum) {
        String nodeHint = String.format(nodeHintTemp, nodeNum);
        String stmt = nodeHint + selectDnGlobalPaxosInfoStmt;
        return stmt;
    }

    protected static String buildChangeLeaderStmtByFollowerNumAndNodeNum(int nodeNUm, String followerAddr) {
        String followerActualAddr = AddressUtils.getStorageNodeAddrByPaxosNodeAddr(followerAddr);
        String stmt = String.format(changeLeaderStmtTemp, nodeNUm, followerActualAddr);
        return stmt;
    }

    private static String buildUpdateStmtOnHaTbl(String dbName, String tbName) {
        String stmt = String.format(HA_TB_DML_TEMP, dbName, tbName);
        return stmt;
    }

    private static void createLogicalDb(Connection conn, String dbName) throws SQLException {
        String createDbStmt = String.format("create database if not exists `%s` mode='auto';", dbName);
        try (final Statement stmt = conn.createStatement()) {
            stmt.execute(createDbStmt);
        }
    }

    private static void createLogicalTblOnDb(Connection conn, String dbName, String tblName) throws SQLException {
        String dropTblStmt = String.format("drop table if exists `%s`.`%s`;", dbName, tblName);
        String createTblStmt =
            String.format("create table if not exists `%s`.`%s` (a int, b int ) broadcast;", dbName, tblName);
        try (final Statement stmt = conn.createStatement()) {
            stmt.execute(dropTblStmt);
        }
        try (final Statement stmt = conn.createStatement()) {
            stmt.execute(createTblStmt);
        }
    }

    private static void createLogicalDbAndTb(Connection conn, String dbName, String tblName) throws SQLException {
        createLogicalDb(conn, dbName);
        createLogicalTblOnDb(conn, dbName, tblName);

        /* enable follower read */
        openFollowerRead(conn);
    }

    protected static void useDbOnConn(Connection conn, String dbName) throws SQLException {
        if (conn != null) {
            String useDbStmt = String.format("use `%s`;", dbName);
            try (final Statement stmt = conn.createStatement()) {
                stmt.execute(useDbStmt);
            }
        }
    }

    private boolean doMultiDnHa(List<Integer> nodeNumList, ClusterInfo clusterInfo, long haOpId,
                                List<Pair<Integer, String>> dnNodeAndFollowerInfoList) throws SQLException {
        for (int i = 0; i < nodeNumList.size(); i++) {
            Integer nodeNum = nodeNumList.get(i);
            DnFollowerFinder followerFinder = new DnFollowerFinder();
            followerFinder.setClusterInfo(clusterInfo);
            followerFinder.setNodeIndex(nodeNum);
            followerFinder.setHaOpId(haOpId);
            followerFinder.execStmt();
            String tarFollower = followerFinder.getTarFollower();
            Pair<Integer, String> nodeAndFoll = new Pair<>(nodeNum, tarFollower);
            dnNodeAndFollowerInfoList.add(nodeAndFoll);
        }

        for (int i = 0; i < dnNodeAndFollowerInfoList.size(); i++) {
            Pair<Integer, String> nodeFollInfo = dnNodeAndFollowerInfoList.get(i);
            Integer nodeNum = nodeFollInfo.getKey();
            String follower = nodeFollInfo.getValue();

            DnLeaderChanger changer = new DnLeaderChanger();
            changer.setClusterInfo(clusterInfo);
            changer.setNodeIndex(nodeNum);
            changer.setNewLeader(follower);
            changer.setHaOpId(haOpId);
            changer.execStmt();
        }

        return true;

    }

    @Test
    public void testDruidHaMultiTimes() throws Exception {
        ClusterInfo clusterInfo = ClusterInfo.getPolarDBXClusterInfoFromEnv();
        testDruidHaInner(20, clusterInfo);
    }

    @Test
    public void testConcurrentDnHaMultiTimes() throws Exception {
        ClusterInfo clusterInfo = ClusterInfo.getPolarDBXClusterInfoFromEnv();
        testConcurrentDnHaInner(3, clusterInfo);
    }

    //    @Test
//    public void testConcurrentDnHaMultiTimesAndCheckFollowerAccess() throws Exception {
//        ClusterInfo clusterInfo = ClusterInfo.getPolarDBXClusterInfoFromEnv();
//        testDruidHaInner(3, clusterInfo);
//    }
    protected static int fetchDnCount(Connection conn) throws SQLException {
        int nodeCnt = 0;
        try (final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery(showDataSources)) {
            while (rs.next()) {
                nodeCnt++;
            }
        }
        return nodeCnt;
    }

    @Data
    protected static class HaStatInfo {
        volatile long haMaxTimeCost = 0;
        volatile long haMinTimeCost = Long.MAX_VALUE;
        volatile long haCount = 0;
        volatile long haTimeCostSum = 0;
    }

    protected void testDruidHaInner(int haTimes, ClusterInfo clusterInfo) throws SQLException, InterruptedException {
        Random random = new Random();

        List<Integer> nodeIndexList = new ArrayList<>();
        HaStatInfo statInfo = new HaStatInfo();
        int waitTimeMs = 3000;

        DnHaEnvBuilder builder = new DnHaEnvBuilder();
        builder.setClusterInfo(clusterInfo);
        builder.execStmt();
        nodeIndexList = builder.getNodeIndexList();

        for (int i = 0; i < haTimes; i++) {
            long haOpId = System.currentTimeMillis();
            List<Integer> haIndexList = new ArrayList<>();
            Integer rndNum = Math.abs(random.nextInt(nodeIndexList.size()));
            Integer haIdx = nodeIndexList.get(rndNum);
            haIndexList.add(haIdx);
            doHaBySpecifyNodeList(haIndexList, clusterInfo, waitTimeMs, haOpId, statInfo);
            String logMsg = String.format("haOpId[%s]: Finish single dn ha for node list on %s time", haOpId, i + 1);
            log.info(logMsg);
            System.out.println(logMsg);
            try {
                Thread.sleep(6000);
            } catch (Throwable ex) {
                // ignore
            }
        }

    }

    protected void testConcurrentDnHaInner(int haTimes, ClusterInfo clusterInfo)
        throws SQLException, InterruptedException {
        List<Integer> nodeIndexList = new ArrayList<>();
        HaStatInfo statInfo = new HaStatInfo();
        int waitTimeMs = 3000;

        DnHaEnvBuilder builder = new DnHaEnvBuilder();
        builder.setClusterInfo(clusterInfo);
        builder.execStmt();
        nodeIndexList = builder.getNodeIndexList();

        for (int i = 0; i < haTimes; i++) {
            long haOpId = System.currentTimeMillis();
            doHaBySpecifyNodeList(nodeIndexList, clusterInfo, waitTimeMs, haOpId, statInfo);
            String logMsg = String.format("haOpId[%s]: Finish multi dn ha for node list on %s time", haOpId, i + 1);
            log.info(logMsg);
            System.out.println(logMsg);
            try {
                Thread.sleep(6000);
            } catch (Throwable ex) {
                // ignore
            }
        }
    }

    protected void testDruidHaInnerWithUpgradeStorageInfoOoVer(int haTimes, ClusterInfo clusterInfo)
        throws SQLException, InterruptedException {
        Random random = new Random();

        List<Integer> nodeIndexList = new ArrayList<>();
        HaStatInfo statInfo = new HaStatInfo();
        int waitTimeMs = 3000;

        // 创建一个定时任务调度器
        ScheduledExecutorService storageInfoUpgradeScheduler = Executors.newScheduledThreadPool(1);

        DnHaEnvBuilder builder = new DnHaEnvBuilder();
        builder.setClusterInfo(clusterInfo);
        builder.execStmt();
        nodeIndexList = builder.getNodeIndexList();

        // 每隔 2 秒执行一次任务
        UpdateStorageInfoOpVerTask updateStorageInfoOpVerTask = new UpdateStorageInfoOpVerTask();
        updateStorageInfoOpVerTask.setClusterInfo(clusterInfo);
        storageInfoUpgradeScheduler.scheduleAtFixedRate(updateStorageInfoOpVerTask, 0, 2,
            TimeUnit.SECONDS); // 初始延迟 0 秒，每隔 2 秒执行一次
        for (int i = 0; i < haTimes; i++) {
            long haOpId = System.currentTimeMillis();
            List<Integer> haIndexList = new ArrayList<>();
            Integer rndNum = Math.abs(random.nextInt(nodeIndexList.size()));
            Integer haIdx = nodeIndexList.get(rndNum);
            haIndexList.add(haIdx);
            doHaBySpecifyNodeList(haIndexList, clusterInfo, waitTimeMs, haOpId, statInfo);
            String logMsg = String.format("haOpId[%s]: Finish single dn ha for node list on %s time", haOpId, i + 1);
            log.info(logMsg);
            System.out.println(logMsg);
            try {
                Thread.sleep(6000);
            } catch (Throwable ex) {
                // ignore
            }
        }
        storageInfoUpgradeScheduler.shutdownNow();
    }

    protected class UpdateStorageInfoOpVerExecutor extends StmtExecutor {

        protected ClusterInfo clusterInfo;

        protected boolean isSucc = false;

        protected String updateStorageOpVerStmt =
            "update metadb.config_listener set op_version=op_version+100 where data_id like 'polardbx.storage.info.%s';";

        protected String reloadStorageOpVerStmt = "alter system reload storage;";

        public void setClusterInfo(ClusterInfo clusterInfo) {
            this.clusterInfo = clusterInfo;
        }

        public UpdateStorageInfoOpVerExecutor() {
        }

        @Override
        protected Connection getConnection() throws SQLException {
            Connection conn =
                DriverManager.getConnection(clusterInfo.getUrl(), clusterInfo.getUser(), clusterInfo.getPassword());
            return conn;
        }

        @Override
        protected void execStmtOnConn(Statement stmt) throws SQLException {
            stmt.executeUpdate(updateStorageOpVerStmt);
            isSucc = true;
        }

        @Override
        protected void execStmtInner(Connection conn) throws SQLException {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(updateStorageOpVerStmt);
            } catch (Throwable ex) {
                handleStmtExMsg(ex);
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(reloadStorageOpVerStmt);
            } catch (Throwable ex) {
                handleStmtExMsg(ex);
            }
            isSucc = true;
        }

        @Override
        protected boolean needRetryConn() {
            return !isSucc;
        }

        @Override
        protected boolean needRetryStmt() {
            return false;
        }
    }

    protected class UpdateStorageInfoOpVerTask implements Runnable {

        protected ClusterInfo clusterInfo;

        public UpdateStorageInfoOpVerTask() {
        }

        @Override
        public void run() {
            try {
                executeUpdateStorageInfoOpVerTask();
                String logMsg =
                    String.format("Succ to upgrade storage info opVer, ts is %s", System.currentTimeMillis());
                log.info(logMsg);
                System.out.println(logMsg);
            } catch (Throwable e) {
                System.err.println("Failed to exec upgrade storage info opVer: " + e.getMessage());
            }
        }

        private void executeUpdateStorageInfoOpVerTask() {
            UpdateStorageInfoOpVerExecutor executor = new UpdateStorageInfoOpVerExecutor();
            executor.setClusterInfo(clusterInfo);
            executor.execStmt();

        }

        public ClusterInfo getClusterInfo() {
            return clusterInfo;
        }

        public void setClusterInfo(ClusterInfo clusterInfo) {
            this.clusterInfo = clusterInfo;
        }
    }

    private String makeContainLeaderByNodeIndex(int nodeIndex,
                                                ClusterInfo clusterInfo) throws SQLException {
        String leader = null;
        String selectDnGlobalStmt = buildSelectDnGlobalPaxosInfoStmtByNodeNum(nodeIndex);
        boolean findLeader = false;
        while (true) {
            try (Connection conn = DriverManager.getConnection(
                clusterInfo.getUrl(), clusterInfo.getUser(), clusterInfo.getPassword())) {
                useDbOnConn(conn, HA_DB_NAME);
                Throwable execEx = null;
                while (true) {
                    final Statement stmt = conn.createStatement();
                    try (final ResultSet rs = stmt.executeQuery(selectDnGlobalStmt)) {
                        int nodes = 0;
                        while (rs.next()) {
                            nodes++;
                            if (rs.getString("ROLE").equalsIgnoreCase("Leader")) {
                                leader = rs.getString("IP_PORT");
                            }
                        }
                        // assume leader
                        Assert.assertNotNull(leader);
                        Assert.assertEquals(3, nodes);
                        findLeader = true;
                        break;
                    } catch (Exception ex) {
                        execEx = ex;
                        log.warn(ex);
                    } finally {
                        if (execEx != null) {
                            break;
                        }
                    }
                }
                if (findLeader) {
                    break;
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (Throwable ex) {
                        // ignore
                    }
                }
            } catch (Throwable ex) {
                log.warn(ex);
                if (findLeader) {
                    break;
                }
            }
        }

        return leader;
    }

    private void doHaBySpecifyNodeList(List<Integer> nodeList,
                                       ClusterInfo clusterInfo,
                                       int waitTimeMs,
                                       long haOpId,
                                       HaStatInfo statInfo) throws SQLException, InterruptedException {
        List<Pair<Integer, String>> dnNodeAndFollowerInfoList = new ArrayList<>();
        doMultiDnHa(nodeList, clusterInfo, haOpId, dnNodeAndFollowerInfoList);
        Thread.sleep(500); // wait for leader change
        checkIfTargetDnHaSuccByExecDml(clusterInfo, waitTimeMs, statInfo, nodeList, haOpId, dnNodeAndFollowerInfoList);
    }

    private void checkIfTargetDnHaSuccByExecDml(ClusterInfo clusterInfo,
                                                int waitTimeMs,
                                                HaStatInfo statInfo,
                                                List<Integer> nodeIndexList,
                                                long haOpId,
                                                List<Pair<Integer, String>> dnNodeAndFollowerInfoList)
        throws SQLException {
        DnFinishHaChecker finishHaChecker = new DnFinishHaChecker();
        finishHaChecker.setClusterInfo(clusterInfo);
        finishHaChecker.setNodeIndexList(nodeIndexList);
        finishHaChecker.setDnNodeAndFollowerInfoList(dnNodeAndFollowerInfoList);
        finishHaChecker.setStatInfo(statInfo);
        finishHaChecker.setHaOpId(haOpId);
        finishHaChecker.execStmt();
    }

    protected static abstract class StmtExecutor {
        public StmtExecutor() {

        }

        protected abstract Connection getConnection() throws SQLException;

        protected abstract boolean needRetryConn();

        protected abstract boolean needRetryStmt();

        protected void execStmtOnConn(Statement stmt) throws SQLException {

        }

        protected void execStmtInner(Connection conn) throws SQLException {
            while (true) {
                try (Statement stmt = conn.createStatement()) {
                    execStmtOnConn(stmt);
                    if (!needRetryStmt()) {
                        break;
                    }
                } catch (Throwable ex) {
                    handleStmtExMsg(ex);
                    if (conn != null && !conn.isClosed()) {
                        if (!needRetryStmt()) {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        protected void handleConnExMsg(Throwable ex) {
            ex.printStackTrace();
            log.warn(ex.getMessage());
        }

        protected void handleStmtExMsg(Throwable ex) {
            ex.printStackTrace();
            log.warn(ex.getMessage());
        }

        public void execStmt() {
            while (true) {
                try (Connection conn = getConnection()) {
                    execStmtInner(conn);
                    if (!needRetryConn()) {
                        break;
                    }
                } catch (Throwable ex) {
                    handleConnExMsg(ex);
                    if (!needRetryConn()) {
                        break;
                    }
                }
            }
        }
    }

    @Data
    public static class DnNewLeaderChecker extends StmtExecutor {

        protected ClusterInfo clusterInfo;
        protected Integer nodeIndex;
        protected String tarLeader = "";
        protected long currFinishHaTimeCost;
        protected HaStatInfo statInfo;
        protected Connection conn;

        protected long haOpId = 0;
        protected volatile boolean findNewLeader = false;
        protected List<String> newestPaxosNodeRoles = new ArrayList<>();

        public DnNewLeaderChecker() {
        }

        @Override
        public void execStmt() {
            if (this.conn == null) {
                super.execStmt();
            } else {
                try {
                    execStmtInner(conn);
                } catch (Throwable ex) {
                    handleConnExMsg(ex);
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        protected void execStmtOnConn(Statement stmt) throws SQLException {
            String selectDnGlobalStmt = buildSelectDnGlobalPaxosInfoStmtByNodeNum(nodeIndex);
            try (final ResultSet rs = stmt.executeQuery(selectDnGlobalStmt)) {
                String newLeader = null;
                int nodes = 0;
                while (rs.next()) {
                    nodes++;
                    String role = rs.getString("ROLE");
                    String ipPort = rs.getString("IP_PORT");
                    Integer weight = rs.getInt("ELECTION_WEIGHT");
                    String nodeWithRole = String.format("[%s:r=%s,w=%s]", ipPort, role, weight);
                    newestPaxosNodeRoles.add(nodeWithRole);
                    if (role.equalsIgnoreCase("Leader")) {
                        newLeader = ipPort;
                    }
                }

                // assume leader
                Assert.assertNotNull(newLeader);
//                Assert.assertEquals(3, nodes);
                Assert.assertEquals(tarLeader, newLeader);

                findNewLeader = true;

                long haCnt = statInfo.haCount;
                long haAvg = statInfo.haTimeCostSum / statInfo.haCount;
                String allNodeWithRolesStr = String.join(",", newestPaxosNodeRoles);
                log.info(String.format(
                    "haOpId[%s]: Check multi-dn ha succ by update stmt for node=%s, roleInfo: %s, haTimeCostMs[cur/max/min/avg,cnt] is [%s/%s/%s/%s, %s],",
                    haOpId, nodeIndex, allNodeWithRolesStr, currFinishHaTimeCost, statInfo.haMaxTimeCost,
                    statInfo.haMinTimeCost,
                    haAvg, haCnt));
            }
        }

        @Override
        protected Connection getConnection() throws SQLException {
            if (this.conn != null) {
                return conn;
            }
            Connection conn =
                DriverManager.getConnection(clusterInfo.getUrl(), clusterInfo.getUser(), clusterInfo.getPassword());
            useDbOnConn(conn, HA_DB_NAME);
            return conn;
        }

        @Override
        protected boolean needRetryConn() {
            return !findNewLeader;
        }

        @Override
        protected boolean needRetryStmt() {
            return !findNewLeader;
        }
    }

    @Data
    public static class DnFinishHaChecker extends StmtExecutor {

        protected ClusterInfo clusterInfo;
        protected Integer nodeIndex;
        protected List<Integer> nodeIndexList = new ArrayList<>();
        protected List<Pair<Integer, String>> dnNodeAndFollowerInfoList = new ArrayList<>();
        protected HaStatInfo statInfo;
        protected long haOpId = 0;

        protected volatile boolean haFinish = false;
        protected long haCurTimeCost = 0;
        protected long haWaitStart = System.currentTimeMillis();
        protected long haWaitEnd = System.currentTimeMillis();

        protected String haTblDmlStmt = null;
        protected volatile boolean findNewFollower = false;

        public DnFinishHaChecker() {
            haTblDmlStmt = buildUpdateStmtOnHaTbl(HA_DB_NAME, HA_TB_NAME);

        }

        @Override
        protected void execStmtOnConn(Statement stmt) throws SQLException {
            stmt.executeUpdate(haTblDmlStmt);
            haWaitEnd = System.currentTimeMillis();
            log.info(
                String.format("haOpId[%s]: exec update[%s] succ for multi-dn", haOpId, haTblDmlStmt));
            haCurTimeCost = haWaitEnd - haWaitStart;
            haFinish = true;
        }

        @Override
        protected void execStmtInner(Connection conn) throws SQLException {
            super.execStmtInner(conn);
            if (haFinish) {
                statInfo.haTimeCostSum += haCurTimeCost;
                statInfo.haCount++;
                if (statInfo.haMaxTimeCost < haCurTimeCost) {
                    statInfo.haMaxTimeCost = haCurTimeCost;
                }
                if (statInfo.haMinTimeCost > haCurTimeCost) {
                    statInfo.haMinTimeCost = haCurTimeCost;
                }
            }
            if (haFinish && !conn.isClosed()) {
                for (int i = 0; i < this.dnNodeAndFollowerInfoList.size(); i++) {
                    Pair<Integer, String> dnNodeAndFoll = this.dnNodeAndFollowerInfoList.get(i);

                    Integer nodeVal = dnNodeAndFoll.getKey();
                    String newLeader = dnNodeAndFoll.getValue();
                    DnNewLeaderChecker newLeaderChecker = new DnNewLeaderChecker();
                    newLeaderChecker.setConn(conn);
                    newLeaderChecker.setClusterInfo(clusterInfo);
                    newLeaderChecker.setCurrFinishHaTimeCost(this.haCurTimeCost);
                    newLeaderChecker.setStatInfo(statInfo);
                    newLeaderChecker.setHaOpId(haOpId);
                    newLeaderChecker.setNodeIndex(nodeVal);
                    newLeaderChecker.setTarLeader(newLeader);
                    newLeaderChecker.execStmt();
                }
            }
        }

        @Override
        protected Connection getConnection() throws SQLException {
            Connection conn =
                DriverManager.getConnection(clusterInfo.getUrlOfCheckIfHaFinished(), clusterInfo.getUser(),
                    clusterInfo.getPassword());
            useDbOnConn(conn, HA_DB_NAME);
            return conn;
        }

        @Override
        protected boolean needRetryConn() {
            return !haFinish;
        }

        @Override
        protected boolean needRetryStmt() {
            return !haFinish;
        }

        @Override
        public void execStmt() {
            this.haWaitStart = System.currentTimeMillis();
            super.execStmt();

        }

    }

    @Data
    public static class DnFollowerFinder extends StmtExecutor {

        protected ClusterInfo clusterInfo;
        protected Integer nodeIndex;
        protected long haOpId = 0;

        protected boolean findFollower = false;
        protected String tarFollower = null;

        public DnFollowerFinder() {
        }

        @Override
        protected void execStmtOnConn(Statement stmt) throws SQLException {

            String follower = null;
            String selectDnGlobalStmt = buildSelectDnGlobalPaxosInfoStmtByNodeNum(nodeIndex);
            log.info(String.format("haOpId[%s]: select global for node %s: %s", haOpId, nodeIndex, selectDnGlobalStmt));
            try (final ResultSet rs = stmt.executeQuery(selectDnGlobalStmt)) {
                while (rs.next()) {
                    Long weight = rs.getLong("ELECTION_WEIGHT");
                    if (weight != null && weight < 4) {
                        continue;
                    }
                    String role = rs.getString("ROLE");
                    String addr = rs.getString("IP_PORT");
                    if (role.equalsIgnoreCase("Follower")) {
                        follower = addr;
                        break;
                    }
                }
            }
            if (null != follower) {
                findFollower = true;
                this.tarFollower = follower;
            }
        }

        @Override
        protected Connection getConnection() throws SQLException {
            Connection conn =
                DriverManager.getConnection(clusterInfo.getUrl(), clusterInfo.getUser(), clusterInfo.getPassword());
            useDbOnConn(conn, HA_DB_NAME);
            return conn;
        }

        @Override
        protected boolean needRetryConn() {
            return !findFollower;
        }

        @Override
        protected boolean needRetryStmt() {
            return !findFollower;
        }

    }

    @Data
    public static class DnLeaderChanger extends StmtExecutor {

        protected ClusterInfo clusterInfo;
        protected Integer nodeIndex;
        protected long haOpId = 0;
        protected String newLeader;

        protected boolean changeRoleSucc = false;

        public DnLeaderChanger() {
        }

        @Override
        protected void execStmtOnConn(Statement stmt) throws SQLException {
            String changeLeaderStmt = buildChangeLeaderStmtByFollowerNumAndNodeNum(nodeIndex, newLeader);
            log.info(String.format("haOpId[%s]:changeLeader for node %s: %s", haOpId, nodeIndex, changeLeaderStmt));
            try {
                stmt.execute(changeLeaderStmt);
            } catch (Throwable ex) {
                String msg = ex.getMessage();
                if (msg.toLowerCase().contains("could not find replica")) {
                    changeRoleSucc = true;
                } else {
                    throw ex;
                }
            }

            changeRoleSucc = true;
        }

        @Override
        protected Connection getConnection() throws SQLException {
            Connection conn =
                DriverManager.getConnection(clusterInfo.getUrl(), clusterInfo.getUser(), clusterInfo.getPassword());
            useDbOnConn(conn, HA_DB_NAME);
            return conn;
        }

        @Override
        protected boolean needRetryConn() {
            return !changeRoleSucc;
        }

        @Override
        protected boolean needRetryStmt() {
            return !changeRoleSucc;
        }

    }

    @Data
    public static class DnHaEnvBuilder extends StmtExecutor {

        protected ClusterInfo clusterInfo;

        protected boolean buildEnvSucc = false;
        protected List<Integer> nodeIndexList = new ArrayList<>();

        public DnHaEnvBuilder() {
        }

        @Override
        protected Connection getConnection() throws SQLException {
            Connection conn =
                DriverManager.getConnection(clusterInfo.getUrl(), clusterInfo.getUser(), clusterInfo.getPassword());
            return conn;
        }

        @Override
        protected boolean needRetryConn() {
            return !buildEnvSucc;
        }

        @Override
        protected boolean needRetryStmt() {
            return !buildEnvSucc;
        }

        @Override
        protected void execStmtInner(Connection conn) throws SQLException {
            createLogicalDbAndTb(conn, HA_DB_NAME, HA_TB_NAME);
            useDbOnConn(conn, HA_DB_NAME);
            int nodeCnt = fetchDnCount(conn);
            for (int i = 0; i < nodeCnt; i++) {
                nodeIndexList.add(i);
            }
            buildEnvSucc = true;
        }

    }

    private static void openFollowerRead(Connection conn) {
        JdbcUtil.executeSuccess(conn, "set session enable_in_memory_follower_read=true;");
    }
}
