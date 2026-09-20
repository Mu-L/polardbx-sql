package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import com.alibaba.polardbx.optimizer.secret.SecretTypeRegistry;
import lombok.Getter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Validates external catalog creation including connectivity testing with rate limiting.
 *
 * <h3>Rate Limiting (Brute-Force Protection)</h3>
 * <p>To prevent password brute-force attacks against remote data sources (e.g., DN instances
 * in the same region without network isolation), this task enforces a per-user error count:
 * <ul>
 *   <li>Each failed connectivity attempt increments the count in MetaDB
 *     ({@code user_login_error_limit} table, key prefix {@code EXT_CATALOG:}).</li>
 *   <li>After {@link #MAX_ERROR_COUNT} failures within {@link #EXPIRE_SECONDS},
 *     further attempts are rejected with "Too many failed connection attempts".</li>
 *   <li>A successful connection clears the count.</li>
 *   <li>An EventLog alert is emitted once when the limit is first reached.</li>
 * </ul>
 *
 * <h3>Emergency Recovery</h3>
 * <p>If a legitimate user is locked out, delete the corresponding MetaDB record directly:
 * <pre>
 * -- Connect to MetaDB and run:
 * DELETE FROM user_login_error_limit WHERE limit_key LIKE 'EXT_CATALOG:%';
 * -- Or for a specific user:
 * DELETE FROM user_login_error_limit WHERE limit_key = 'EXT_CATALOG:USERNAME@HOST';
 * </pre>
 */
@Getter
@TaskName(name = "ExternalCatalogValidateTask")
public class ExternalCatalogValidateTask extends BaseValidateTask {

    private static final int CONNECTIVITY_TIMEOUT_SECONDS = 30;
    private static final int MAX_ERROR_COUNT = 5;
    private static final long EXPIRE_SECONDS = 86400;
    private static final int SHUTDOWN_GRACE_SECONDS = 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalCatalogValidateTask.class);

    private static final String SELECT_RATE_LIMIT =
        "SELECT error_count, expire_date FROM user_login_error_limit WHERE limit_key = ?";
    private static final String UPSERT_RATE_LIMIT =
        "INSERT INTO user_login_error_limit (limit_key, max_error_limit, error_count, expire_date) "
            + "VALUES (?, ?, 1, ?) ON DUPLICATE KEY UPDATE "
            + "error_count = CASE "
            + "WHEN expire_date IS NULL OR expire_date < ? THEN 1 "
            + "ELSE error_count + 1 END, "
            + "expire_date = VALUES(expire_date), max_error_limit = VALUES(max_error_limit)";
    private static final String SELECT_COUNT_AFTER_UPSERT =
        "SELECT error_count FROM user_login_error_limit WHERE limit_key = ?";
    private static final String DELETE_RATE_LIMIT =
        "DELETE FROM user_login_error_limit WHERE limit_key = ?";

    private final String catalogName;
    private final String connector;
    private final byte[] encryptedProperties;
    private final String secretName;

    public ExternalCatalogValidateTask(String catalogName, String connector,
                                       byte[] encryptedProperties, String secretName) {
        super(SystemDbHelper.DEFAULT_DB_NAME);
        this.catalogName = catalogName;
        this.connector = connector;
        this.encryptedProperties = encryptedProperties;
        this.secretName = secretName;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            String limitKey = getLimitKey(executionContext);

            checkRateLimit(metaDbConn, limitKey);

            ExternalNameValidator.checkLegacyPossibleExternalCatalog(metaDbConn);

            ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor();
            accessor.setConnection(metaDbConn);
            if (accessor.selectByName(catalogName) != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                    "External catalog '" + catalogName + "' already exists");
            }

            Map<String, String> props = ExternalCredentialEncryptor.decryptToMap(encryptedProperties);

            SecretBundle secret = SecretBundle.EMPTY;
            if (!ExternalCatalogConstants.isMockConnector(connector)) {
                ExternalSecretAccessor sa = new ExternalSecretAccessor();
                sa.setConnection(metaDbConn);
                if (sa.selectByName(secretName) == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                        "Secret '" + secretName + "' does not exist");
                }

                SecretManager.SecretInfo info = SecretManager.getInstance().getInfo(secretName);
                if (info != null) {
                    PropertyDefinition def = SecretTypeRegistry.getInstance().get(info.type);
                    if (def != null) {
                        def.validateAsCatalog(props);
                    }
                }

                secret = SecretManager.getInstance().resolve(secretName, props);
            }

            try (ConnectorMetadata metadata = ConnectorRegistry.getInstance()
                .get(connector).createMetadata(props, secret)) {
                checkConnectivity(metadata);
            } catch (Exception e) {
                incrementErrorCount(metaDbConn, limitKey);
                throw e;
            }
            clearErrorCount(metaDbConn, limitKey);
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                "Validation failed: " + e.getMessage());
        }
    }

    private void checkConnectivity(ConnectorMetadata metadata) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<List<String>> future = executor.submit(() -> metadata.listDatabases());
            try {
                future.get(CONNECTIVITY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                throw e;
            }
        } catch (TimeoutException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "Connection to '" + catalogName + "' timed out after "
                    + CONNECTIVITY_TIMEOUT_SECONDS + "s");
        } catch (Exception e) {
            Throwable cause = (e instanceof ExecutionException && e.getCause() != null)
                ? e.getCause() : e;
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                "Failed to connect to '" + catalogName + "' (connector=" + connector + "): "
                    + cause.getMessage());
        } finally {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(SHUTDOWN_GRACE_SECONDS, TimeUnit.SECONDS)) {
                    LOGGER.warn("Connectivity check thread for catalog '" + catalogName
                        + "' did not terminate after " + SHUTDOWN_GRACE_SECONDS
                        + "s, may leak remote connection");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private String getLimitKey(ExecutionContext executionContext) {
        String user = "unknown";
        String host = "unknown";
        if (executionContext != null && executionContext.getPrivilegeContext() != null) {
            user = executionContext.getPrivilegeContext().getUser();
            host = executionContext.getPrivilegeContext().getHost();
        }
        return "EXT_CATALOG:" + (user == null ? "unknown" : user.toUpperCase()) + "@" + host;
    }

    private void checkRateLimit(Connection conn, String limitKey) {
        try (PreparedStatement ps = conn.prepareStatement(SELECT_RATE_LIMIT)) {
            ps.setString(1, limitKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int errorCount = rs.getInt("error_count");
                    Timestamp expireDate = rs.getTimestamp("expire_date");
                    if (errorCount >= MAX_ERROR_COUNT
                        && expireDate != null
                        && expireDate.getTime() > System.currentTimeMillis()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                            "Too many failed connection attempts. Please try again later.");
                    }
                }
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            // rate limit check failure should not block normal flow
        }
    }

    private void incrementErrorCount(Connection conn, String limitKey) {
        try {
            long nowMillis = System.currentTimeMillis();
            Timestamp now = new Timestamp(nowMillis);
            Timestamp expireDate = new Timestamp(nowMillis + EXPIRE_SECONDS * 1000);
            try (PreparedStatement ps = conn.prepareStatement(UPSERT_RATE_LIMIT)) {
                ps.setString(1, limitKey);
                ps.setInt(2, MAX_ERROR_COUNT);
                ps.setTimestamp(3, expireDate);
                ps.setTimestamp(4, now);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(SELECT_COUNT_AFTER_UPSERT)) {
                ps.setString(1, limitKey);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        int newCount = rs.getInt("error_count");
                        if (newCount == MAX_ERROR_COUNT) {
                            EventLogger.log(EventType.EXT_COL_ERR,
                                "External catalog rate limit reached: " + limitKey);
                        }
                    }
                }
            }
        } catch (Exception e) {
            // increment failure should not block normal flow
        }
    }

    private void clearErrorCount(Connection conn, String limitKey) {
        try (PreparedStatement ps = conn.prepareStatement(DELETE_RATE_LIMIT)) {
            ps.setString(1, limitKey);
            ps.executeUpdate();
        } catch (Exception e) {
            // clear failure should not block normal flow
        }
    }

}
