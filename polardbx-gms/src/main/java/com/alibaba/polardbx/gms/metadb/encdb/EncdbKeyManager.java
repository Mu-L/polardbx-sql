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

package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * MEK只持久化存储其哈希摘要值，内存中缓存MEK
 * 注意保证mek和mekHash的一致性
 *
 * @author pangzhaoxing
 */
public class EncdbKeyManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(EncdbKeyManager.class);

    private static final EncdbKeyManager INSTANCE = new EncdbKeyManager();

    public static final int KMS_MEK_VERSION = 0;

    private volatile byte[] localMek = null;

    private volatile byte[] localMekHash = null;

    private long mekId = -1;

    // for kms mode
    private volatile String kmsEncMek = null;

    private volatile String kmsIv = null;

    private volatile byte[] kmsPlainMek = null;

    private volatile byte[] kmsMekHash = null;

    private volatile String kmsRegion = null;

    private volatile String kmsKeyId = null;

    public static EncdbKeyManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        reloadEncKeys();
        setupConfigListener();
    }

//    private synchronized void reloadEncKeys() {
//        try (Connection conn = MetaDbUtil.getConnection()) {
//            EncdbKeyAccessor accessor = new EncdbKeyAccessor();
//            accessor.setConnection(conn);
//            EncdbKey encdbKey = accessor.getMekHash();
//            if (encdbKey != null) {
//                this.mekHash = Utils.base64ToBytes(encdbKey.getKey());
//                this.mek = null;
//                this.mekId = encdbKey.getId();
//            }
//        } catch (SQLException e) {
//            MetaDbLogUtil.META_DB_LOG.error(e);
//            throw GeneralUtil.nestedException(e);
//        }
//    }

    private synchronized void reloadEncKeys() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            EncdbKeyAccessor accessor = new EncdbKeyAccessor();
            accessor.setConnection(conn);
            byte[] newLocalMekHash = null;
            byte[] newLocalMek = null;
            long newMekId = -1;
            String newKmsEncMek = null;
            String newKmsIv = null;
            byte[] newKmsMekHash = null;
            byte[] newKmsPlainMek = null;
            String newKmsRegion = null;
            String newKmsKeyId = null;

            List<EncdbKey> encdbKeyList = accessor.selectAll();
            for (EncdbKey encdbKey : encdbKeyList) {
                switch (EncdbKey.KeyType.valueOf(encdbKey.getType())) {
                case MEK_HASH:
                    newLocalMekHash = Utils.base64ToBytes(encdbKey.getKey());
                    newMekId = encdbKey.getId();
                    break;
                case KMS_ENC_MEK:
                    newKmsEncMek = encdbKey.getKey();
                    break;
                case KMS_IV:
                    newKmsIv = encdbKey.getKey();
                    break;
                case KMS_MEK_HASH:
                    newKmsMekHash = Utils.base64ToBytes(encdbKey.getKey());
                    break;
                case KMS_REGION:
                    newKmsRegion = encdbKey.getKey();
                    break;
                case KMS_KEY_ID:
                    newKmsKeyId = encdbKey.getKey();
                    break;
                }
            }

            this.localMekHash = newLocalMekHash;
            this.localMek = newLocalMek;
            this.mekId = newMekId;
            this.kmsEncMek = newKmsEncMek;
            this.kmsIv = newKmsIv;
            this.kmsMekHash = newKmsMekHash;
            this.kmsPlainMek = newKmsPlainMek;
            this.kmsRegion = newKmsRegion;
            this.kmsKeyId = newKmsKeyId;

        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public void replaceEncKeys(List<EncdbKey> encdbKeyList) {
        replaceEncKeys(encdbKeyList, Collections.emptyList());
    }

    public void replaceEncKeys(List<EncdbKey> encdbKeyList, List<EncdbKey.KeyType> encdbKeyTypesToDelete) {
        if (encdbKeyTypesToDelete == null) {
            encdbKeyTypesToDelete = Collections.emptyList();
        }
        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            EncdbKeyAccessor accessor = new EncdbKeyAccessor();
            accessor.setConnection(conn);
            for (EncdbKey.KeyType encdbKeyType : encdbKeyTypesToDelete) {
                accessor.deleteByType(encdbKeyType.name());
            }
            accessor.replace(encdbKeyList);
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID, conn);
            conn.commit();
            // wait for all cn to load metadb
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID);
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public void deleteEncKeysByType(List<EncdbKey.KeyType> encdbKeyTypeList) {
        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            EncdbKeyAccessor accessor = new EncdbKeyAccessor();
            accessor.setConnection(conn);
            for (EncdbKey.KeyType encdbKeyType : encdbKeyTypeList) {
                accessor.deleteByType(encdbKeyType.name());
            }
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID, conn);
            conn.commit();
            // wait for all cn to load metadb
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID);
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    private void setupConfigListener() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            EncdbKeyManager.EncdbKeyConfigListener listener = new EncdbKeyConfigListener();
            MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID, conn);
            MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID, listener);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "setup encdb key config_listener failed");
        }
    }

    protected static class EncdbKeyConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            EncdbKeyManager.getInstance().reloadEncKeys();
        }
    }

    /**
     * 获取mek
     */
    public byte[] getMek() {
        if (useKmsMode()) {
            return kmsPlainMek;
        } else {
            return localMek;
        }
    }

    public byte[] getLocalMek() {
        return localMekHash;
    }

    /**
     * mek和mekHash必须保持一致，所以对两者进行操作时，需要加锁保持修改的原子性
     */
    public synchronized boolean verifyMek(byte[] mek, boolean kmsMode) throws NoSuchAlgorithmException {
        if (kmsMode) {
            // kms mode
            if (kmsMekHash == null) {
                throw new EncdbException("kmsMekHash is null");
            } else {
                if (Arrays.equals(kmsMekHash, createMekHash(mek))) {
                    this.kmsPlainMek = mek;
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            if (localMekHash == null) {
                throw new EncdbException("mekHash is null");
            } else {
                if (Arrays.equals(localMekHash, createMekHash(mek))) {
                    this.localMek = mek;
                    return true;
                } else {
                    return false;
                }
            }
        }

    }

    public long getMekId() {
        return mekId;
    }

    public byte[] getLocalMekHash() {
        return localMekHash;
    }

    public String getKmsEncMek() {
        return kmsEncMek;
    }

    public String getKmsIv() {
        return kmsIv;
    }

    public byte[] getKmsPlainMek() {
        return kmsPlainMek;
    }

    public byte[] getKmsMekHash() {
        return kmsMekHash;
    }

    public String getKmsRegion() {
        return kmsRegion;
    }

    public String getKmsKeyId() {
        return kmsKeyId;
    }

    public static byte[] createMekHash(byte[] mekBytes) throws NoSuchAlgorithmException {
        return SecurityUtil.calcMysqlUserPassword(mekBytes);
    }

    public boolean useKmsMode() {
        return kmsEncMek != null && kmsMekHash != null && kmsRegion != null && kmsKeyId != null && InstConfUtil.getBool(
            ConnectionParams.ENCDB_ENABLE_KMS_MODE);
    }
}
