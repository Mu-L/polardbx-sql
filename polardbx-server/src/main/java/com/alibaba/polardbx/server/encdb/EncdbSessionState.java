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

package com.alibaba.polardbx.server.encdb;

import com.alibaba.polardbx.common.encdb.enums.CCFlags;
import com.alibaba.polardbx.common.encdb.enums.HashAlgo;
import com.alibaba.polardbx.common.encdb.enums.Constants;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;

/**
 * @author pangzhaoxing
 */
public class EncdbSessionState {

    static {
        Provider sunjceProvider = Security.getProvider("SUNJCE");
        if (sunjceProvider != null) {
            Security.addProvider(sunjceProvider);
        }
        BouncyCastleProvider bouncyCastleProvider = new BouncyCastleProvider();
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(bouncyCastleProvider);
        }
    }

    private HashAlgo hashAlgo;

    private Constants.EncAlgo encAlgo;

    private CCFlags ccFlags;

    private byte[] dek;

    private byte[] nonce;

    private Cipher cipher;

    private SecureRandom secureRandom;

    public EncdbSessionState(HashAlgo hashAlgo, Constants.EncAlgo encAlgo, CCFlags ccFlags, byte[] dek, byte[] nonce) {
        this.hashAlgo = hashAlgo;
        this.encAlgo = encAlgo;
        this.ccFlags = ccFlags;
        this.dek = dek;
        this.nonce = nonce;
        this.secureRandom = new SecureRandom();
        initCipher();
    }

    public void initCipher() {
        try {
            switch (encAlgo) {
            case AES_128_GCM:
            case AES_256_GCM:
                this.cipher = Cipher.getInstance("AES/GCM/NoPadding");
                break;
            case AES_128_ECB:
            case AES_256_ECB:
                this.cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
                break;
            case AES_128_CBC:
            case AES_256_CBC:
                this.cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
                break;
            case SM4_128_ECB:
                this.cipher = Cipher.getInstance("SM4/ECB/PKCS5Padding");
                break;
            case SM4_128_CBC:
                this.cipher = Cipher.getInstance("SM4/CBC/PKCS5Padding");
                break;
            case SM4_128_GCM:
                this.cipher = Cipher.getInstance("SM4/GCM/NoPadding");
                break;
            case AES_128_CTR:
            case AES_256_CTR:
                this.cipher = Cipher.getInstance("AES/CTR/NoPadding");
                break;
            case SM4_128_CTR:
                this.cipher = Cipher.getInstance("SM4/CTR/NoPadding");
                break;
            default:
                throw new NoSuchAlgorithmException("Unsupported algorithm " + encAlgo.name());
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public CCFlags getCcFlags() {
        return ccFlags;
    }

    public HashAlgo getHashAlgo() {
        return hashAlgo;
    }

    public Constants.EncAlgo getEncAlgo() {
        return encAlgo;
    }

    public byte[] getDek() {
        return dek;
    }

    public void setDek(byte[] dek) {
        this.dek = dek;
    }

    public byte[] getNonce() {
        return nonce;
    }

    public Cipher getCipher() {
        return cipher;
    }

    public SecureRandom getSecureRandom() {
        return secureRandom;
    }
}
