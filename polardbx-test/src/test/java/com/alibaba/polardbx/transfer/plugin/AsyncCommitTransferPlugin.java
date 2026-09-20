package com.alibaba.polardbx.transfer.plugin;

import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author wuzhe
 */
public class AsyncCommitTransferPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(AsyncCommitTransferPlugin.class);
    private final boolean injectCommitFailure;
    private final double injectCommitFailureProb;
    private final SecureRandom random = new SecureRandom();

    public AsyncCommitTransferPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("transfer_async_commit");
        if (null == config) {
            enabled = false;
            injectCommitFailure = false;
            injectCommitFailureProb = 0;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
        injectCommitFailure = config.getBoolean("inject_commit_failure", false);
        injectCommitFailureProb = config.getDouble("inject_commit_failure_prob", 0.1);
    }

    @Override
    public void runInternal() {
        getConnectionAndExecute(dsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                long src = random.nextInt(rowCount);
                long dst = random.nextInt(rowCount);
                while (src == dst) {
                    dst = random.nextInt(rowCount);
                }

                transfer(random, stmt, src, dst);
            } catch (SQLException e) {
                error.set(e);
                logger.error("Transfer simple error.", e);
            }
        });
    }

    private void transfer(SecureRandom random, Statement stmt, long src, long dst) throws SQLException {
        // begin
        stmt.execute("set transaction_policy=TSO, enable_async_commit_80=true, enable_tso_opt=false");
        stmt.execute("begin");
        boolean injectError = false;

        try {
            // check balance is enough
            ResultSet rs = stmt.executeQuery("select balance from accounts where id = " + src + " for update");

            if (!rs.next() || rs.getLong(1) < 0) {
                stmt.execute("rollback");
                return;
            }

            // transfer
            String hint = "";
            if (injectCommitFailure && random.nextFloat() < injectCommitFailureProb) {
                injectError = true;
                float choice = random.nextFloat();
                if (choice < 0.075) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_2') */";
                } else if (choice < 0.15) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_3') */";
                } else if (choice < 0.225) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_4') */";
                } else if (choice < 0.3) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_5') */";
                } else if (choice < 0.375) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_10') */";
                } else if (choice < 0.45) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_11') */";
                } else if (choice < 0.525) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_12') */";
                } else if (choice < 0.6) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_13') */";
                } else if (choice < 0.675) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_14') */";
                } else if (choice < 0.75) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_15') */";
                } else if (choice < 0.825) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_16') */";
                } else if (choice < 0.9) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_17') */";
                } else if (choice < 0.92) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_6') */";
                } else if (choice < 0.94) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_7') */";
                } else if (choice < 0.96) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_8') */";
                } else {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='AC_FLAG_9') */";
                }
            }
            stmt.execute(hint + "update accounts SET balance = balance - 1, version = version + 1 where id = " + src);
            stmt.execute(hint + "update accounts SET balance = balance + 1, version = version + 1 where id = " + dst);
            // commit
            stmt.execute("commit");
        } catch (SQLException e) {
            if (!injectError && !e.getMessage().contains("Deadlock found when trying to get lock")) {
                logger.error("Transfer async commit error.", e);
            }
            stmt.execute("rollback ");
        }
    }
}
