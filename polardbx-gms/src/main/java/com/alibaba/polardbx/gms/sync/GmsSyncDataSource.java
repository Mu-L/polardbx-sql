/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.sync;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

public class GmsSyncDataSource extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(GmsSyncDataSource.class);

    // connectTimeout/socketTimeout bound the establishment and read phases so that a broken
    // manager port cannot hang the sync thread indefinitely.
    private static final MessageFormat MYSQL_URL_FORMAT =
        new MessageFormat("jdbc:mysql://{0}:{1}/{2}?useSSL=false&connectTimeout=3000&socketTimeout=60000");

    private static final String SYNC_PASSWORD = "none";
    private static final String SYNC_DATABASE = "sync";

    // Sync traffic is bursty and manager-port connections are scarce, so keep the pool small.
    private static final int POOL_MAX_ACTIVE = 5;
    private static final long POOL_MAX_WAIT_MS = 5000L;
    // Short backoff between pool-level physical connection attempts so transient jitter is
    // retried quickly while the caller is still waiting for a connection.
    private static final long TIME_BETWEEN_CONNECT_ERROR_MS = 100L;
    // The manager port already understands this probe (ClusterSyncManager uses it before SYNC).
    private static final String VALIDATION_QUERY = "show @@config";

    private final String host;
    private final String port;
    private final String jdbcUrl;
    private final Properties connInfo;
    private DruidDataSource dataSource;

    public GmsSyncDataSource(String instId, String host, String port) {
        this.host = host;
        this.port = port;
        this.jdbcUrl = MYSQL_URL_FORMAT.format(new String[] {host, port, SYNC_DATABASE});
        this.connInfo = new Properties();
        this.connInfo.setProperty("user", instId);
        this.connInfo.setProperty("password", SYNC_PASSWORD);
    }

    @Override
    protected void doInit() {
        super.doInit();
        // Change context:
        // - Before: every sync call created a fresh physical connection through
        //   DriverManager.getConnection without pooling, retry or timeouts; the simple direct
        //   connection was sufficient while sync traffic was low, but its zero-tolerance behavior
        //   turned transient CN-to-CN network jitter into DDL SyncTask failures and paused DDL
        //   jobs (AONE-85095101).
        // - Path impact: only the ClusterSyncManager sync paths that call getConnection() change;
        //   the local in-process sync path, DN/MetaDB connections and the DDL engine retry policy
        //   are untouched. Connections are now pooled per manager endpoint and validated with the
        //   same "show @@config" probe the sync path already issued manually.
        // - Capability regression: none expected; the pool stays small (maxActive 5), failures
        //   still surface as SQLException after bounded retries, and init no longer performs any
        //   network I/O (initialSize 0), so node topology reloads remain cheap.
        DruidDataSource druidDataSource = new DruidDataSource() {
            @Override
            public PhysicalConnectionInfo createPhysicalConnection() throws SQLException {
                // Fault injection hook for tests; keeps failing at the physical establishment
                // boundary exactly like a real network jitter would.
                GmsSyncConnectionFailInjector.injectIfNeeded();
                return super.createPhysicalConnection();
            }
        };
        druidDataSource.setName("GmsSync-" + host + "_" + port);
        druidDataSource.setUrl(jdbcUrl);
        druidDataSource.setUsername(connInfo.getProperty("user"));
        druidDataSource.setPassword(connInfo.getProperty("password"));
        druidDataSource.setInitialSize(0);
        druidDataSource.setMinIdle(0);
        druidDataSource.setMaxActive(POOL_MAX_ACTIVE);
        druidDataSource.setMaxWait(POOL_MAX_WAIT_MS);
        druidDataSource.setTestOnBorrow(true);
        druidDataSource.setTestWhileIdle(false);
        druidDataSource.setTestOnReturn(false);
        druidDataSource.setValidationQuery(VALIDATION_QUERY);
        druidDataSource.setBreakAfterAcquireFailure(false);
        druidDataSource.setTimeBetweenConnectErrorMillis(TIME_BETWEEN_CONNECT_ERROR_MS);
        try {
            druidDataSource.init();
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
        this.dataSource = druidDataSource;
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
        }
    }

    public Connection getConnection() throws SQLException {
        // Change context:
        // - Before: a single DriverManager attempt meant any transient establishment failure was
        //   thrown straight to the sync caller, and the DDL engine burned its few retries within
        //   milliseconds because DDL_TASK_ERROR_RETRY_WAIT_TIME defaults to 0.
        // - Path impact: every ClusterSyncManager manager-port sync now absorbs short jitter
        //   inside this retry loop (configurable via GMS_SYNC_CONNECTION_RETRY_TIMES /
        //   GMS_SYNC_CONNECTION_RETRY_INTERVAL_MS); callers that exhaust the retries still get a
        //   SQLException with unchanged semantics, and non-sync users of GmsNode do not go
        //   through this class at all.
        // - Capability regression: worst-case sync latency grows by at most
        //   retryTimes * (pool maxWait + retryInterval); a permanently dead node is still
        //   reported as a sync failure instead of hanging, since every attempt is bounded.
        int retryTimes = Math.max(1, DynamicConfig.getInstance().getGmsSyncConnectionRetryTimes());
        long retryIntervalMs = DynamicConfig.getInstance().getGmsSyncConnectionRetryIntervalMs();
        SQLException lastException = null;
        for (int attempt = 1; attempt <= retryTimes; attempt++) {
            try {
                return dataSource.getConnection();
            } catch (SQLException e) {
                lastException = e;
                logger.warn("Failed to establish gms sync connection (attempt " + attempt + "/" + retryTimes
                    + "), url: " + jdbcUrl + ", cause: " + e.getMessage());
                if (attempt < retryTimes && retryIntervalMs > 0) {
                    try {
                        Thread.sleep(retryIntervalMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }
            }
        }
        throw lastException;
    }

}
