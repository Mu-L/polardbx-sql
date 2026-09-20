package com.alibaba.polardbx.server.encdb;

import com.alibaba.polardbx.common.encdb.cipher.CipherForMySQL;
import com.alibaba.polardbx.common.encdb.cipher.SymCrypto;
import com.alibaba.polardbx.common.encdb.enums.CCFlags;
import com.alibaba.polardbx.common.encdb.enums.Constants;
import com.alibaba.polardbx.common.encdb.utils.HashUtil;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.google.common.primitives.Bytes;
import org.bouncycastle.crypto.CryptoException;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.encdb.utils.Utils.swapBytesByPivot;

/**
 * @author pangzhaoxing
 */
public class EncdbSessionCipher extends CipherForMySQL {

    EncdbSessionState state;

    Constants.EncAlgo algo;

    public EncdbSessionCipher(int type, EncdbSessionState encdbSessionState) {
        super(type, encdbSessionState.getEncAlgo());
        this.state = encdbSessionState;
        this.algo = state.getEncAlgo();
    }

    @Override
    public byte[] encrypt(CCFlags flag, byte[] key, byte[] inputPlain, byte[] nonce)
        throws NoSuchAlgorithmException, CryptoException {

        List<Byte> encBytes = new ArrayList<>();

        encBytes.add((byte) 0);//for check code
        encBytes.add(VERSION);
        encBytes.add((byte) getEncType());
        encBytes.add((byte) state.getEncAlgo().getVal());
        encBytes.addAll(Bytes.asList(nonce));

        byte checkCode = xorArray(Bytes.asList(inputPlain));

        byte[] inputWCheckCode = new byte[inputPlain.length + 1];
        System.arraycopy(inputPlain, 0, inputWCheckCode, 0, inputPlain.length);
        inputWCheckCode[inputWCheckCode.length - 1] = checkCode;

        //prepare iv
        byte[] iv = generateSessionIv(flag, inputWCheckCode);

        //actual encryption
        byte[] tmpData = null;
        switch (algo) {
        case AES_128_GCM:
        case AES_256_GCM:
            encBytes.addAll(Bytes.asList(iv));
            tmpData = SymCrypto.aesGcmEncrypt(key, inputWCheckCode, iv, state.getCipher());
            // Java format: DATA || TAG, convert to EncDB CipherV0 format: TAG || DATA
            encBytes.addAll(swapBytesByPivot(tmpData, tmpData.length - SymCrypto.GCMTagLength));
            break;
        case AES_128_ECB:
        case AES_256_ECB:
            encBytes.addAll(Bytes.asList(SymCrypto.aesECBEncrypt(key, inputWCheckCode, state.getCipher())));
            break;
        case AES_128_CBC:
        case AES_256_CBC:
            encBytes.addAll(Bytes.asList(iv));
            encBytes.addAll(Bytes.asList(SymCrypto.aesCBCEncrypt(key, inputWCheckCode, iv, state.getCipher())));
            break;
        case SM4_128_ECB:
            encBytes.addAll(Bytes.asList(SymCrypto.sm4ECBEncrypt(key, inputWCheckCode, state.getCipher())));
            break;
        case SM4_128_CBC:
            encBytes.addAll(Bytes.asList(iv));
            List<Byte> tmp = Bytes.asList(SymCrypto.sm4CBCEncrypt(key, inputWCheckCode, iv, state.getCipher()));
            encBytes.addAll(tmp);
            break;
        case SM4_128_GCM:
            encBytes.addAll(Bytes.asList(iv));
            tmpData = SymCrypto.sm4GcmEncrypt(key, inputWCheckCode, iv, state.getCipher());
            encBytes.addAll(swapBytesByPivot(tmpData, tmpData.length - SymCrypto.GCMTagLength));
            break;
        case AES_128_CTR:
        case AES_256_CTR:
            encBytes.addAll(Bytes.asList(iv));
            encBytes.addAll(Bytes.asList(SymCrypto.aesCTREncrypt(key, inputWCheckCode, iv, state.getCipher())));
            break;
        case SM4_128_CTR:
            encBytes.addAll(Bytes.asList(iv));
            encBytes.addAll(Bytes.asList(SymCrypto.sm4CTREncrypt(key, inputWCheckCode, iv, state.getCipher())));
            break;
        default:
            throw new NoSuchAlgorithmException("Unsupported algorithm " + algo.name());
        }

        byte[] res = Bytes.toArray(encBytes);
        res[0] = xorArray(res, 1, res.length - 1);//check code;

        return res;
    }

    private byte[] generateSessionIv(CCFlags flag, byte[] dataIn) throws NoSuchAlgorithmException {
        int ivLen = 0;
        switch (algo) {
        case AES_128_GCM:
        case AES_256_GCM:
        case SM4_128_GCM:
            ivLen = SymCrypto.GCMIVLength;
            if (flag == CCFlags.DET) {
                byte[] tmp = HashUtil.doSHA256(dataIn);
                List<Byte> ivTmp = Bytes.asList(tmp);
                assert ivLen <= tmp.length;
                return Bytes.toArray(ivTmp.subList(0, ivLen));
            } else {
                return Utils.generateIv(ivLen, state.getSecureRandom());
            }
        case AES_128_ECB:
        case AES_256_ECB:
        case SM4_128_ECB:
            break;
        case AES_128_CBC:
        case AES_256_CBC:
        case SM4_128_CBC:
            ivLen = SymCrypto.CBCIVLength;
            if (flag == CCFlags.DET) {
                /*result is 32 bytes iv*/
                byte[] tmp =
                    (algo == Constants.EncAlgo.SM4_128_CBC) ? HashUtil.doSM3(dataIn) :
                        HashUtil.doSHA256(dataIn);

                List<Byte> ivTmp = Bytes.asList(tmp);
                assert ivLen <= tmp.length;
                return Bytes.toArray(ivTmp.subList(0, ivLen));
            } else {
                return Utils.generateIv(ivLen, state.getSecureRandom());
            }
        case AES_128_CTR:
        case AES_256_CTR:
        case SM4_128_CTR:
            ivLen = SymCrypto.CTRIVLength;
            if (flag == CCFlags.DET) {
                byte[] tmp =
                    (algo == Constants.EncAlgo.SM4_128_CTR) ? HashUtil.doSM3(dataIn) :
                        HashUtil.doSHA256(dataIn);
                List<Byte> ivTmp = Bytes.asList(tmp);
                assert ivLen <= tmp.length;
                return Bytes.toArray(ivTmp.subList(0, ivLen));
            } else {
                return Utils.generateIv(ivLen, state.getSecureRandom());
            }
        default:
            throw new NoSuchAlgorithmException("Unsupported algorithm " + algo.name());
        }
        return new byte[0];
    }

}
