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

package com.alibaba.polardbx.server.encdb.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.cipher.CipherSuite;
import com.alibaba.polardbx.common.encdb.cipher.Envelope;
import com.alibaba.polardbx.common.encdb.enums.Constants;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.sync.EncdbMekProvisionSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKey;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.encdb.EncdbServer;
import com.alibaba.polardbx.server.encdb.EncdbSessionState;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author pangzhaoxing
 */
public class EncdbMekProvisionHandler implements EncdbHandler {

    private final Logger logger = LoggerFactory.getLogger(EncdbMekProvisionHandler.class);

    @Override
    public JSONObject handle(JSONObject request, ServerConnection serverConnection) {
        try {
            String cipherSuiteStr = request.getString(MsgKeyConstants.CIPHER_SUITE);
            CipherSuite cipherSuite = cipherSuiteStr == null ?
                EncdbServer.cipherSuite :
                new CipherSuite(EncdbServer.teeType, request.getString(MsgKeyConstants.CIPHER_SUITE));
            String algoStr = request.getString(MsgKeyConstants.ALGORITHM);
            Constants.EncAlgo encAlgo = algoStr == null ?
                EncdbServer.getInstance().getCipherSuite().getSymmAlgo() :
                Constants.EncAlgo.valueOf(request.getString(MsgKeyConstants.ALGORITHM));
            byte[] envBytes = Utils.base64ToBytes(request.getString(MsgKeyConstants.ENVELOPE));
            Envelope env = Envelope.fromBytes(envBytes);
            env.setCiperSuite(cipherSuite);
            JSONObject envJson = JSON.parseObject(
                new String(env.open(EncdbServer.getInstance().getPemPrivateKey()), StandardCharsets.UTF_8));
            String mek = envJson.getString(MsgKeyConstants.MEK);//already base64 encoded
            byte[] mekBytes = Utils.base64ToBytes(mek);

            verifyOrSetMek(mekBytes, serverConnection);

            //如果用户设置了全局加密算法，则不使用用户传入的加密算法
            String encdbEncAlgo = InstConfUtil.getOriginVal(ConnectionParams.ENCDB_ENCRYPTION_ALGORITHM);
            if (!TStringUtil.isEmpty(encdbEncAlgo)) {
                encAlgo = Constants.EncAlgo.valueOf(encdbEncAlgo);
            }
            serverConnection.setEncdbSessionState(initEncdbSessionState(encAlgo));
            logger.info("[ENCDB]: mek provision success");
        } catch (Exception e) {
            throw new EncdbException(e);
        }

        return EMPTY;
    }

    public EncdbSessionState initEncdbSessionState(Constants.EncAlgo encAlgo) {
        byte[] nonce = EncdbServer.createNonce();
        byte[] mek = EncdbKeyManager.getInstance().getMek();
        byte[] dek = EncdbServer.createDEK(EncdbServer.cipherSuite.getHashAlgo(), mek, nonce, encAlgo);
        return new EncdbSessionState(EncdbServer.cipherSuite.getHashAlgo(), encAlgo, EncdbServer.ccFlags, dek, nonce);
    }

    public void verifyOrSetMek(byte[] mekBytes, ServerConnection serverConnection) throws NoSuchAlgorithmException {
        boolean useKmsMode = EncdbKeyManager.getInstance().useKmsMode();
        //如果不用kmsmode，且没有注册过mek
        if (!useKmsMode && EncdbKeyManager.getInstance().getLocalMekHash() == null) {
            registerLocalMek(mekBytes);
        } else {
            if (!EncdbKeyManager.getInstance().verifyMek(mekBytes, useKmsMode)) {
                throw new EncdbException("the mek is wrong");
            }
        }
    }

    public static void registerLocalMek(byte[] mekBytes) throws NoSuchAlgorithmException {
        byte[] mekHash = EncdbKeyManager.createMekHash(mekBytes);
        EncdbKeyManager.getInstance().replaceEncKeys(
            Collections.singletonList(new EncdbKey(Utils.bytesTobase64(mekHash), EncdbKey.KeyType.MEK_HASH)));
        SyncManagerHelper.syncThrowExceptions(new EncdbMekProvisionSyncAction(mekBytes, false), SyncScope.ALL);
    }

    /**
     * 只能通过管控生成KMS mek，再进行注册
     */
    public static void registerKmsMek(String kmsEncMek, byte[] kmsPlainMekBytes, String kmsRegion, String keyId)
        throws NoSuchAlgorithmException {
        registerKmsMek(kmsEncMek, kmsPlainMekBytes, kmsRegion, keyId, null);
    }

    public static void registerKmsMek(String kmsEncMek, byte[] kmsPlainMekBytes, String kmsRegion, String keyId,
                                      String kmsIv) throws NoSuchAlgorithmException {
        byte[] kmsMekHash = EncdbKeyManager.createMekHash(kmsPlainMekBytes);
        List<EncdbKey> encdbKeyList = new ArrayList<>();
        encdbKeyList.add(new EncdbKey(kmsEncMek, EncdbKey.KeyType.KMS_ENC_MEK));
        if (kmsIv != null) {
            encdbKeyList.add(new EncdbKey(kmsIv, EncdbKey.KeyType.KMS_IV));
        }
        encdbKeyList.add(new EncdbKey(Utils.bytesTobase64(kmsMekHash), EncdbKey.KeyType.KMS_MEK_HASH));
        encdbKeyList.add(new EncdbKey(kmsRegion, EncdbKey.KeyType.KMS_REGION));
        encdbKeyList.add(new EncdbKey(keyId, EncdbKey.KeyType.KMS_KEY_ID));
        List<EncdbKey.KeyType> encdbKeyTypesToDelete =
            kmsIv == null ? Collections.singletonList(EncdbKey.KeyType.KMS_IV) : Collections.emptyList();
        EncdbKeyManager.getInstance().replaceEncKeys(encdbKeyList, encdbKeyTypesToDelete);
        SyncManagerHelper.syncThrowExceptions(new EncdbMekProvisionSyncAction(kmsPlainMekBytes, true), SyncScope.ALL);
    }

}
