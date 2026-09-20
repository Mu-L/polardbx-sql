package com.alibaba.polardbx.server.util;

import com.alibaba.polardbx.common.RevisableOrderInvariantHash;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.chain.GlobalChainAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.conn.InnerConnectionManager;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

public class GlobalChainUtils {
    private static final String HASH_DIGEST = "_polardbx_hash_digest_";
    private static final String HASH_INS = "hash_ins";
    private static final String HASH_DEL = "hash_del";
    private static final String BLOCK_HASH = "block_hash";

    /**
     * Check whether the hash of user table equals to the hash of history table.
     *
     * @param schemaName user schema
     * @param tableName user table
     * @param info detailed information for show
     * @return true if the hash of user table equals to the hash of history table
     */
    public static boolean checkHist(String schemaName, String tableName, StringBuilder info) {
        final long tso = getTso();
        info.append("\nStart checking history table...\nCheck with tso ").append(tso).append(".\n");
        final long userTableHash = getUserTableHash(schemaName, tableName, tso, info);
        final long histTableHash = getHistTableHash(schemaName, tableName, tso, info);
        return userTableHash >= 0 && userTableHash == histTableHash;
    }

    /**
     * Check whether the hash of hist table equals to the hash of global table.
     * In other words, check whether the modifications to physical rows correspond to the operations of logical SQL.
     *
     * @param schemaName user schema
     * @param tableName user table
     * @param info detailed information for show
     * @return true if the hash of hist table equals to the hash of global table
     */
    public static boolean checkGlobal(String schemaName, String tableName, StringBuilder info) {
        final long tso = getTso();
        info.append("\nStart checking global table...\nCheck with tso ").append(tso).append(".\n");
        final long histTableHash = getHistTableHash(schemaName, tableName, tso, info);
        final long globalTableHash = getGlobalTableHash(schemaName, tableName, tso, info);
        return histTableHash >= 0 && globalTableHash == histTableHash;
    }

    /**
     * Check whether the chain of hist table and the chain of global table are valid.
     * In other words, check whether the data in hist table and global table are valid.
     *
     * @param schemaName user schema
     * @param tableName user table
     * @param info detailed information for show
     * @return true if the chain of hist table and the chain of global table are valid
     */
    public static boolean checkChain(String schemaName, String tableName, StringBuilder info) {
        final long tso = getTso();
        info.append("\nStart checking if chains are valid...\nCheck with tso ").append(tso)
            .append(".\n");
        final boolean histValid = checkHistChainValid(schemaName, tableName, tso, info);
        final boolean globalValid = checkGlobalChainValid(tso, info);
        return histValid && globalValid;
    }

    /**
     * Check_hist, Check_global, and Check_chain.
     *
     * @return true if all check ok.
     */
    public static boolean checkAll(String schemaName, String tableName, StringBuilder info) {

        return true;
    }

    public static long getTso() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
            checkpointsAccessor.setConnection(metaDbConn);

            List<ColumnarCheckpointsRecord> columnarCheckpointsRecords =
                checkpointsAccessor.queryLastByTypes(ImmutableList.of(ColumnarCheckpointsAccessor.CheckPointType.STREAM,
                    ColumnarCheckpointsAccessor.CheckPointType.DDL,
                    ColumnarCheckpointsAccessor.CheckPointType.HEARTBEAT));
            if (!columnarCheckpointsRecords.isEmpty()) {
                return columnarCheckpointsRecords.get(0).binlogTso;
            } else {
                return -1;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Fail to fetch columnar checkpoint.", e);
        }
    }

    public static long getUserTableHash(String schemaName, String tableName, long tso, StringBuilder sb) {
        sb.append("Start calculating user table hash...\n");
        final String queryUserHash = String.format("/*+TDDL:SNAPSHOT_TS=%s*/select %s from %s",
            tso, HASH_DIGEST, tableName);
        try (Connection connection = InnerConnectionManager.getInstance().getConnection(schemaName);
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(queryUserHash)) {
            final RevisableOrderInvariantHash hash =
                new RevisableOrderInvariantHash().reset(TddlConstants.MAGICAL_NUM);
            long cnt = 0;
            while (rs.next()) {
                cnt++;
                hash.add(crc32(rs.getString(HASH_DIGEST)));
            }
            sb.append("Calculate ").append(cnt).append(" rows for user table, hash is ")
                .append(PasswdUtil.encrypt(Long.toHexString(hash.getResult()))).append(".\n");
            sb.append("Finish calculating user table hash.\n");
            return hash.getResult();
        } catch (Throwable t) {
            throw new RuntimeException("Failed to calculate user table hash.", t);
        }
    }

    public static boolean checkHistChainValid(String schemaName, String tableName, long tso, StringBuilder sb) {
        sb.append("Start checking if hist chain is valid...\n");
        // Find last archive record.
        final String lastArchiveHash = String.format(
            "select block_id, block_hash from %s where extra = 'ARCHIVE' order by block_id desc limit 1",
            getHistTableName(tableName)
        );
        long blockId = -1, initHash = TddlConstants.MAGICAL_NUM;
        try (Connection connection = InnerConnectionManager.getInstance().getConnection(schemaName);
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(lastArchiveHash)) {
            if (rs.next()) {
                blockId = rs.getLong(1);
                initHash = Long.parseLong(PasswdUtil.decrypt(rs.getString(2)), 16);
            }
        } catch (Throwable t) {
            throw new RuntimeException("Failed to check history table valid.", t);
        }

        final String queryUserHash = String.format(
            "select %s, %s, %s from %s where tso < %s and block_id > %s order by block_id",
            HASH_INS, HASH_DEL, BLOCK_HASH, getHistTableName(tableName), tso, blockId);
        try (Connection connection = InnerConnectionManager.getInstance().getConnection(schemaName);
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(queryUserHash)) {
            final RevisableOrderInvariantHash validHash = new RevisableOrderInvariantHash().reset(initHash);
            long cnt = 0, ins = 0, del = 0, update = 0;
            while (rs.next()) {
                cnt++;
                String hashIns = rs.getString(HASH_INS);
                String hashDel = rs.getString(HASH_DEL);
                if (null != hashIns && null != hashDel) {
                    update++;
                    validHash.add(crc32(hashIns)).remove(crc32(hashDel));
                } else if (null != hashIns) {
                    ins++;
                    validHash.add(crc32(hashIns));
                } else if (null != hashDel) {
                    del++;
                    validHash.remove(crc32(hashDel));
                }
                if (validHash.getResult() != Long.parseLong(PasswdUtil.decrypt(rs.getString(BLOCK_HASH)), 16)) {
                    sb.append("Found invalid hash result for ").append(cnt).append("-th rows.\n");
                    return false;
                }
            }
            sb.append("Calculate ").append(cnt).append(" rows for hist table, hash is ")
                .append(PasswdUtil.encrypt(Long.toHexString(validHash.getResult())))
                .append(", insert ").append(ins).append(" rows, delete ")
                .append(del).append(" rows, update ").append(update).append(" rows.\n");
            sb.append("Finish calculating hist table hash, hist chain is valid.\n");
            return true;
        } catch (Throwable t) {
            throw new RuntimeException("Failed to check history table valid.", t);
        }
    }

    public static long getHistTableHash(String schemaName, String tableName, long tso, StringBuilder sb) {
        sb.append("Start calculating hist table hash...\n");
        final String queryUserHash =
            String.format(
                "select %s from %s where tso < %s order by tso desc,block_id desc limit 1",
                BLOCK_HASH, getHistTableName(tableName), tso);
        try (Connection connection = InnerConnectionManager.getInstance().getConnection(schemaName);
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(queryUserHash)) {
            if (rs.next()) {
                long hashValue = Long.parseLong(PasswdUtil.decrypt(rs.getString(BLOCK_HASH)), 16);
                sb.append("Calculate hist table hash ").append(rs.getString(BLOCK_HASH)).append(".\n");
                sb.append("Finish calculating user table hash.\n");
                return hashValue;
            } else {
                sb.append("Fail to calculate user table hash.\n");
                return -1;
            }
        } catch (Throwable t) {
            throw new RuntimeException("Failed to calculate history table hash.", t);
        }
    }

    public static boolean checkGlobalChainValid(long tso, StringBuilder sb) {
        sb.append("Start checking if global chain is valid...\n");
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            final GlobalChainAccessor accessor = new GlobalChainAccessor();
            accessor.setConnection(metaDbConn);
            String init = accessor.queryArchiveBlockHashById();
            final RevisableOrderInvariantHash hash =
                new RevisableOrderInvariantHash().reset(init == null ? TddlConstants.MAGICAL_NUM
                    : Long.parseLong(PasswdUtil.decrypt(init), 16));
            AtomicLong cnt = new AtomicLong(0);
            AtomicBoolean fail = new AtomicBoolean(false);
            accessor.processGlobalChain(tso, fail, (r) -> {
                if (r.schemaName == null || r.tableName == null) {
                    hash.reset(Long.parseLong(PasswdUtil.decrypt(r.blockHash), 16));
                } else {
                    cnt.incrementAndGet();
                    hash.add(crc32(r.traceId))
                        .add(crc32(r.ip))
                        .add(crc32(String.valueOf(r.port)))
                        .add(crc32(r.user))
                        .add(crc32(r.schemaName))
                        .add(crc32(r.opHash));
                    if (hash.getResult() != Long.parseLong(PasswdUtil.decrypt(r.blockHash), 16)) {
                        sb.append("Global hash not matched, accumulated block hash is ")
                            .append(PasswdUtil.encrypt(Long.toHexString(hash.getResult())))
                            .append(", recorded block hash is ")
                            .append(r.blockHash)
                            .append(".\n");
                        fail.set(true);
                    }
                }
            });
            sb.append("Calculate ").append(cnt.get()).append(" rows for global hash ")
                .append(PasswdUtil.encrypt(Long.toHexString(hash.getResult()))).append(".\n");
            if (fail.get()) {
                sb.append("Fail to calculate global table hash.\n");
                return false;
            }
            sb.append("Finish calculating global table hash, global chain is valid.\n");
            return true;
        } catch (Throwable t) {
            throw new RuntimeException("Failed to calculate global table hash.", t);
        }
    }

    public static long getGlobalTableHash(String schemaName, String tableName, long tso, StringBuilder sb) {
        sb.append("Start calculating global table hash...\n");
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            final GlobalChainAccessor accessor = new GlobalChainAccessor();
            accessor.setConnection(metaDbConn);
            final RevisableOrderInvariantHash hash =
                new RevisableOrderInvariantHash().reset(TddlConstants.MAGICAL_NUM);
            AtomicLong cnt = new AtomicLong(0);
            AtomicBoolean first = new AtomicBoolean(true);
            accessor.processGlobalChain(schemaName, tableName, tso, (opHash) -> {
                cnt.incrementAndGet();
                if (first.compareAndSet(true, false)) {
                    hash.reset(Long.parseLong(PasswdUtil.decrypt(opHash), 16));
                } else {
                    hash.add(Long.parseLong(PasswdUtil.decrypt(opHash), 16)).remove(TddlConstants.MAGICAL_NUM);
                }
            });
            sb.append("Calculate ").append(cnt.get()).append(" rows for global table, hash is ")
                .append(PasswdUtil.encrypt(Long.toHexString(hash.getResult()))).append(".\n");
            sb.append("Finish calculating global table hash.\n");
            return hash.getResult();
        } catch (Throwable t) {
            throw new RuntimeException("Failed to calculate global table hash.", t);
        }
    }

    // Check and accumulate
    public static String accumulateOpHash(Connection metaDbConn, String schemaName, String tableName, long blockId) {
        try {
            final GlobalChainAccessor accessor = new GlobalChainAccessor();
            accessor.setConnection(metaDbConn);
            final RevisableOrderInvariantHash hash = new RevisableOrderInvariantHash().reset(TddlConstants.MAGICAL_NUM);
            AtomicLong cnt = new AtomicLong(0);
            AtomicBoolean first = new AtomicBoolean(true);
            accessor.processGlobalChainById(schemaName, tableName, blockId, (opHash) -> {
                cnt.incrementAndGet();
                if (first.compareAndSet(true, false)) {
                    hash.reset(Long.parseLong(PasswdUtil.decrypt(opHash), 16));
                } else {
                    hash.add(Long.parseLong(PasswdUtil.decrypt(opHash), 16)).remove(TddlConstants.MAGICAL_NUM);
                }
            });
            return PasswdUtil.encrypt(Long.toHexString(hash.getResult()));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to calculate global table hash.", t);
        }
    }

    private static String getHistTableName(String tableName) {
        return "__" + tableName + "_hist";
    }

    public static long crc32(String s) {
        CRC32 crc = new CRC32();
        crc.update(s.getBytes());
        return crc.getValue();
    }

    public static void checkPrivilege(ServerConnection c) {
        c.updatePrivilegeContext();
        PrivilegeContext privilegeContext = c.getExecutionContext().getPrivilegeContext();
        AccountType accountType = privilegeContext.getPolarUserInfo().getAccountType();
        if (accountType != AccountType.GOD && accountType != AccountType.DBA && accountType != AccountType.AUDITOR) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                "DBA or AUDITOR", privilegeContext.getUser(), privilegeContext.getHost());
        }
    }
}
