package com.alibaba.polardbx.qatest.transfer;

import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import lombok.Setter;
import lombok.experimental.Accessors;

public class TransferTestBase {

    @Setter
    @Accessors(chain = true)
    static class TransferConfigBuilder {
        String dsn = null;
        String replicaDsn = null;
        String cdcDsn = null;
        String connProperties = null;
        String runMode = "local";
        long rowCount = 100;
        long initialBalance = 100000;
        String createTableSuffix = null;
        long reportInterval = 5;
        long timeout = 60;

        boolean enableTransferSimple = false;
        long transferThreads = 5;
        boolean injectCommitFailure = false;
        double injectCommitFailureProb = 0.1;

        boolean enableTransferTsoOpt = false;
        long tsoOptTransferThreads = 5;
        boolean tsoOptInjectCommitFailure = false;
        double tsoOptInjectCommitFailureProb = 0.1;

        boolean enableTransferAsyncCommit = false;
        long asyncCommitTransferThreads = 5;
        boolean asyncCommitInjectCommitFailure = false;
        double asyncCommitInjectCommitFailureProb = 0.1;

        boolean enableCheckBalance = false;
        long checkThreads = 2;
        String beforeCheckStmt = null;

        boolean enableReplicaRead = false;
        long replicaCheckThreads = 2;
        String replicaReadHint = null;
        String replicaSessionVar = null;
        boolean replicaEnableStrongConsistency = true;

        boolean enableFlashbackQuery = false;
        long flashbackCheckThreads = 2;
        long flashbackMinSeconds = 10;
        long flashbackMaxSeconds = 20;

        boolean enableReplicaFlashbackQuery = false;
        long replicaFlashbackCheckThreads = 2;
        long replicaFlashbackMinSeconds = 10;
        long replicaFlashbackMaxSeconds = 20;
        String replicaFlashbackReadHint = null;

        boolean enableCheckCdc = false;
        long checkCdcThreads = 2;
        String cdcBeforeCheckStmt = null;

        TransferConfig build() {
            TransferConfig transferConfig = new TransferConfig();
            transferConfig.dsn = dsn;
            transferConfig.conn_properties = connProperties;
            transferConfig.runmode = runMode;
            transferConfig.row_count = rowCount;
            transferConfig.initial_balance = initialBalance;
            transferConfig.create_table_suffix = createTableSuffix;
            transferConfig.report_interval = reportInterval;
            transferConfig.timeout = timeout;

            transferConfig.transfer_simple.enabled = enableTransferSimple;
            transferConfig.transfer_simple.threads = transferThreads;
            transferConfig.transfer_simple.inject_commit_failure = injectCommitFailure;
            transferConfig.transfer_simple.inject_commit_failure_prob = injectCommitFailureProb;

            transferConfig.transfer_tso_opt.enabled = enableTransferTsoOpt;
            transferConfig.transfer_tso_opt.threads = tsoOptTransferThreads;
            transferConfig.transfer_tso_opt.inject_commit_failure = tsoOptInjectCommitFailure;
            transferConfig.transfer_tso_opt.inject_commit_failure_prob = tsoOptInjectCommitFailureProb;

            transferConfig.transfer_async_commit.enabled = enableTransferAsyncCommit;
            transferConfig.transfer_async_commit.threads = asyncCommitTransferThreads;
            transferConfig.transfer_async_commit.inject_commit_failure = asyncCommitInjectCommitFailure;
            transferConfig.transfer_async_commit.inject_commit_failure_prob = asyncCommitInjectCommitFailureProb;

            transferConfig.check_balance.enabled = enableCheckBalance;
            transferConfig.check_balance.threads = checkThreads;
            transferConfig.check_balance.before_check_stmt = beforeCheckStmt;

            transferConfig.replica_read.enabled = enableReplicaRead;
            transferConfig.replica_read.threads = replicaCheckThreads;
            transferConfig.replica_read.replica_dsn = replicaDsn;
            transferConfig.replica_read.replica_read_hint = replicaReadHint;
            transferConfig.replica_read.session_var = replicaSessionVar;
            transferConfig.replica_read.replica_strong_consistency = replicaEnableStrongConsistency;

            transferConfig.flashback_query.enabled = enableFlashbackQuery;
            transferConfig.flashback_query.threads = flashbackCheckThreads;
            transferConfig.flashback_query.min_seconds = flashbackMinSeconds;
            transferConfig.flashback_query.max_seconds = flashbackMaxSeconds;

            transferConfig.replica_flashback_query.enabled = enableReplicaFlashbackQuery;
            transferConfig.replica_flashback_query.threads = replicaFlashbackCheckThreads;
            transferConfig.replica_flashback_query.replica_dsn = replicaDsn;
            transferConfig.replica_flashback_query.replica_read_hint = replicaFlashbackReadHint;
            transferConfig.replica_flashback_query.min_seconds = replicaFlashbackMinSeconds;
            transferConfig.replica_flashback_query.max_seconds = replicaFlashbackMaxSeconds;

            transferConfig.check_cdc.enabled = enableCheckCdc;
            transferConfig.check_cdc.replica_dsn = cdcDsn;
            transferConfig.check_cdc.threads = checkCdcThreads;
            transferConfig.check_cdc.before_check_stmt = cdcBeforeCheckStmt;

            return transferConfig;
        }
    }

    static TransferConfigBuilder newPolarDBXTransferConfigBuilder() {
        // some default value for polarDB-X
        String dsn = PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_USER)
            + ":"
            + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PASSWORD)
            + "@tcp("
            + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS)
            + ":"
            + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PORT)
            + ")/transfer_test";
        return new TransferConfigBuilder()
            .setDsn(dsn)
            .setReplicaDsn(dsn)
            .setConnProperties(PropertiesUtil.getConnectionProperties())
            .setRowCount(PropertiesUtil.transferRowCount)
            .setTimeout(PropertiesUtil.transferTestTime * 60)
            .setBeforeCheckStmt("set transaction_policy = TSO")
            .setReplicaReadHint("/*+TDDL:SLAVE()*/")
            .setReplicaFlashbackReadHint("/*+TDDL:SLAVE()*/")
            .setCreateTableSuffix(PropertiesUtil.configProp.getProperty(ConfigConstant.CREATE_TABLE_SUFFIX));
    }

    static class TransferConfig {
        String dsn;
        String conn_properties;
        String runmode = "local";
        long row_count = 100;
        long initial_balance = 1000;
        String create_table_suffix;
        long report_interval = 5;
        long timeout;

        TransferSimple transfer_simple = new TransferSimple();
        TransferTsoOpt transfer_tso_opt = new TransferTsoOpt();
        TransferAsyncCommit transfer_async_commit = new TransferAsyncCommit();
        CheckBalance check_balance = new CheckBalance();
        ReplicaRead replica_read = new ReplicaRead();
        FlashbackQuery flashback_query = new FlashbackQuery();
        ReplicaFlashbackQuery replica_flashback_query = new ReplicaFlashbackQuery();
        CheckCdc check_cdc = new CheckCdc();
    }

    static class TransferSimple {
        boolean enabled = false;
        long threads = 5;
        boolean inject_commit_failure = false;
        double inject_commit_failure_prob = 0.1;
    }

    static class TransferTsoOpt {
        boolean enabled = false;
        long threads = 5;
        boolean inject_commit_failure = true;
        double inject_commit_failure_prob = 0.1;
    }

    static class TransferAsyncCommit {
        boolean enabled = false;
        long threads = 5;
        boolean inject_commit_failure = true;
        double inject_commit_failure_prob = 0.1;
    }

    static class CheckBalance {
        boolean enabled = false;
        long threads = 2;
        String before_check_stmt;
    }

    static class ReplicaRead {
        boolean enabled = false;
        long threads = 2;
        String replica_read_hint;
        String replica_dsn;
        String session_var;
        boolean replica_strong_consistency = true;
    }

    static class FlashbackQuery {
        boolean enabled = false;
        long threads = 2;
        long min_seconds = 10;
        long max_seconds = 20;
    }

    static class ReplicaFlashbackQuery {
        boolean enabled = false;
        long threads = 2;
        long min_seconds = 10;
        long max_seconds = 20;
        String replica_read_hint;
        String replica_dsn;
    }

    static class CheckCdc {
        boolean enabled = false;
        long threads = 2;
        String replica_dsn;
        String before_check_stmt;
    }
}
