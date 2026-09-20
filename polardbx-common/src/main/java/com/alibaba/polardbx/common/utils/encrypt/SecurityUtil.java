package com.alibaba.polardbx.common.utils.encrypt;

import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * 加密解密工具类
 * <p>
 * MySQL4.1以后版本, 数据库保存的密码mysql.user.Password是用SHA1加密的：SHA1(SHA1(明文密码))
 * 1、服务器发送随机字符串（scramble）给客户端.
 * 2、客户端作如下计算:
 * stage1_hash = SHA1(明文密码).
 * token = SHA1(scramble + SHA1(stage1_hash)) XOR stage1_hash
 * 3、客户端将token发送给服务端
 * 4、服务端作如下计算：
 * stage1_hash = token XOR SHA1(scramble + mysql.user.Password)
 * 5、服务端比对SHA1(stage1_hash)和mysql.user.Password，如果匹配，则认证正确。
 * <p>
 * 注意：SHA1(A+B)意思是SHA1（A字符串连接B字符串）
 * <p>
 * 在权限和账号系统之前tddl服务端存储的密码有两种:
 * 1. 极老的版本存储的是明文
 * 2. 最近的版本存储的是经过一次SHA-1哈希的密码(stage1_hash)
 * mysql存储的是经过两次SHA1哈希的密码, 大部分tddl服务端存储的密码是经过1次SHA-1哈希的密码,
 * 两种方式区别不大, 针对常用密码, 通过事先计算常用密码表的SHA1哈希值(彩虹表),
 * 常用密码存在被破解的可能.最安全的方式是加盐存储, 这样用户即使看到服务端存储的mysql.user.Password
 * 也需要针对每个盐值重新计算彩虹表(算法复杂度就上升到O(N^2)), 这也是互联网公司常用的存储用户密码的方式.
 * <p>
 * 此外: 在权限和账号系统之前的tddl对于密码认证的算法是不完整的, 采取的仍然是客户端的加密算法, 就是上面的第二步.
 * 正确的做法走上面的第四步算出stage1_hash, 然后根据第五步做匹配.我把这些观察记下来, 方面后人理解.
 * 在权限和账号系统中我会采用mysql的正式算法, 但是依然兼容老的tddl的方式.
 *
 * @author xianmao.hexm 2010-4-14 下午03:22:05
 * @author arnkore 2016-11-24 16:42
 */
public class SecurityUtil {

    private static final int CACHING_SHA2_DIGEST_LENGTH = 32;

    // SHA-256迭代次数
    private static final int ROUNDS_DEFAULT = 5000;

    // SHA-256块大小
    private static final int MIXCHARS = 32;

    /**
     * 获取数据库保存的密码mysql.user.Password
     *
     * @param plainPassword 明文密码
     */
    public static byte[] calcMysqlUserPassword(byte[] plainPassword) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(plainPassword);
        md.reset();
        return md.digest(pass1);
    }

    /**
     * mysql服务端认证密码算法
     *
     * @param token 经过客户端加密算法算出的密码
     * @param mysqlUserPassword 数据库保存的密码
     * @param scramble 服务器发送给客户端的随机字符串
     */
    public static boolean verify(byte[] token, byte[] mysqlUserPassword, byte[] scramble)
        throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        md.update(scramble);
        byte[] stage1_hash = md.digest(mysqlUserPassword);
        for (int i = 0; i < stage1_hash.length; i++) {
            stage1_hash[i] = (byte) (stage1_hash[i] ^ token[i]);
        }

        md.reset();
        byte[] candidate_hash2 = md.digest(stage1_hash);
        boolean match = true;

        if (mysqlUserPassword.length != candidate_hash2.length) {
            match = false;
        }

        for (int i = 0; i < candidate_hash2.length; i++) {
            if (candidate_hash2[i] != mysqlUserPassword[i]) {
                match = false;
                break;
            }
        }

        return match;
    }

    /**
     * tddl部分老的密码是按照这种方式保存的, 而mysql的方式是保存两重SHA-1哈希的值.
     */
    public static final byte[] sha1Pass(byte[] pass) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        return md.digest(pass);
    }

    public static byte[] scrambleCachingSha2(byte[] password, byte[] seed) throws DigestException {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new DigestException(ex);
        }

        byte[] dig1 = new byte[CACHING_SHA2_DIGEST_LENGTH];
        byte[] dig2 = new byte[CACHING_SHA2_DIGEST_LENGTH];
        byte[] scramble1 = new byte[CACHING_SHA2_DIGEST_LENGTH];

        // SHA2(src) => digest_stage1
        md.update(password, 0, password.length);
        md.digest(dig1, 0, CACHING_SHA2_DIGEST_LENGTH);
        md.reset();

        // SHA2(digest_stage1) => digest_stage2
        md.update(dig1, 0, dig1.length);
        md.digest(dig2, 0, CACHING_SHA2_DIGEST_LENGTH);
        md.reset();

        // SHA2(digest_stage2, m_rnd) => scramble_stage1
        md.update(dig2, 0, dig1.length);
        md.update(seed, 0, seed.length);
        md.digest(scramble1, 0, CACHING_SHA2_DIGEST_LENGTH);

        // XOR(digest_stage1, scramble_stage1) => scramble
        byte[] mysqlScrambleBuff = new byte[CACHING_SHA2_DIGEST_LENGTH];
        xorString(dig1, mysqlScrambleBuff, scramble1, CACHING_SHA2_DIGEST_LENGTH);

        return mysqlScrambleBuff;
    }

    /**
     * 仍然是4.1以后版本客户端加密算法, 只不过参数是经过一次SHA-1哈希的密码.
     */
    public static final byte[] scramble411BySha1Pass(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {
        // pass已经是经过一次sha-1计算的密文
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(pass);
        md.reset();
        md.update(seed);
        byte[] pass2 = md.digest(pass1);
        for (int i = 0; i < pass2.length; i++) {
            pass2[i] = (byte) (pass2[i] ^ pass[i]);
        }
        return pass2;
    }

    /**
     * 4.1以后版本, 客户端的加密计算算法
     *
     * @param pass 明文密码
     * @param seed 服务器发送给客户端的随机字符串
     */
    public static final byte[] scramble411(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(pass);
        md.reset();
        byte[] pass2 = md.digest(pass1);
        md.reset();
        md.update(seed);
        byte[] pass3 = md.digest(pass2);
        for (int i = 0; i < pass3.length; i++) {
            pass3[i] = (byte) (pass3[i] ^ pass1[i]);
        }
        return pass3;
    }

    public static final String scramble323(String pass, String seed) {
        if ((pass == null) || (pass.length() == 0)) {
            return pass;
        }
        byte b;
        double d;
        long[] pw = hash(seed);
        long[] msg = hash(pass);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];
        for (int i = 0; i < seed.length(); i++) {
            seed1 = ((seed1 * 3) + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) java.lang.Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }
        seed1 = ((seed1 * 3) + seed2) % max;
        seed2 = (seed1 + seed2 + 33) % max;
        d = (double) seed1 / (double) max;
        b = (byte) java.lang.Math.floor(d * 31);
        for (int i = 0; i < seed.length(); i++) {
            chars[i] ^= (char) b;
        }
        return new String(chars);
    }

    private static long[] hash(String src) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;
        for (int i = 0; i < src.length(); ++i) {
            switch (src.charAt(i)) {
            case ' ':
            case '\t':
                continue;
            default:
                tmp = (0xff & src.charAt(i));
                nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
                nr2 += ((nr2 << 8) ^ nr);
                add += tmp;
            }
        }
        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;
        return result;
    }

    /**
     * bytes转换成十六进制字符串
     */
    public static String byte2HexStr(byte[] b) {
        StringBuilder hs = new StringBuilder();
        for (int n = 0; n < b.length; n++) {
            String hex = (Integer.toHexString(b[n] & 0XFF));
            if (hex.length() == 1) {
                hs.append("0" + hex);
            } else {
                hs.append(hex);
            }
        }

        return hs.toString();
    }

    /**
     * bytes转换成十六进制字符串
     */
    public static byte[] hexStr2Bytes(String src) {
        if (src == null) {
            return null;
        }
        int offset = 0;
        int length = src.length();
        if (length == 0) {
            return new byte[0];
        }

        boolean odd = length << 31 == Integer.MIN_VALUE;
        byte[] bs = new byte[odd ? (length + 1) >> 1 : length >> 1];
        for (int i = offset, limit = offset + length; i < limit; ++i) {
            char high, low;
            if (i == offset && odd) {
                high = '0';
                low = src.charAt(i);
            } else {
                high = src.charAt(i);
                low = src.charAt(++i);
            }
            int b;
            switch (high) {
            case '0':
                b = 0;
                break;
            case '1':
                b = 0x10;
                break;
            case '2':
                b = 0x20;
                break;
            case '3':
                b = 0x30;
                break;
            case '4':
                b = 0x40;
                break;
            case '5':
                b = 0x50;
                break;
            case '6':
                b = 0x60;
                break;
            case '7':
                b = 0x70;
                break;
            case '8':
                b = 0x80;
                break;
            case '9':
                b = 0x90;
                break;
            case 'a':
            case 'A':
                b = 0xa0;
                break;
            case 'b':
            case 'B':
                b = 0xb0;
                break;
            case 'c':
            case 'C':
                b = 0xc0;
                break;
            case 'd':
            case 'D':
                b = 0xd0;
                break;
            case 'e':
            case 'E':
                b = 0xe0;
                break;
            case 'f':
            case 'F':
                b = 0xf0;
                break;
            default:
                throw new IllegalArgumentException("illegal hex-string: " + src);
            }
            switch (low) {
            case '0':
                break;
            case '1':
                b += 1;
                break;
            case '2':
                b += 2;
                break;
            case '3':
                b += 3;
                break;
            case '4':
                b += 4;
                break;
            case '5':
                b += 5;
                break;
            case '6':
                b += 6;
                break;
            case '7':
                b += 7;
                break;
            case '8':
                b += 8;
                break;
            case '9':
                b += 9;
                break;
            case 'a':
            case 'A':
                b += 10;
                break;
            case 'b':
            case 'B':
                b += 11;
                break;
            case 'c':
            case 'C':
                b += 12;
                break;
            case 'd':
            case 'D':
                b += 13;
                break;
            case 'e':
            case 'E':
                b += 14;
                break;
            case 'f':
            case 'F':
                b += 15;
                break;
            default:
                throw new IllegalArgumentException("illegal hex-string: " + src);
            }
            bs[(i - offset) >> 1] = (byte) b;
        }
        return bs;
    }

    private static void xorString(byte[] from, byte[] to, byte[] scramble, int length) {
        int pos = 0;
        int scrambleLength = scramble.length;
        while (pos < length) {
            to[pos] = (byte) (from[pos] ^ scramble[pos % scrambleLength]);
            pos++;
        }
    }

    // for sha2 password
    public static byte[] scrambleSHA2(byte[] plaintext, byte[] salt, int rounds) throws NoSuchAlgorithmException {
        // 初始化算法参数
        String digestAlgorithm = "SHA-256";
        MessageDigest digestA = MessageDigest.getInstance(digestAlgorithm);
        MessageDigest digestB = MessageDigest.getInstance(digestAlgorithm);

        // 步骤1-3: 初始化Digest A，添加密码和盐
        digestA.update(plaintext);
        digestA.update(salt);

        // 步骤4-8: 生成Digest B
        digestB.update(plaintext);
        digestB.update(salt);
        digestB.update(plaintext);
        byte[] B = digestB.digest();

        // 步骤9-10: 分块添加B到Digest A
        int passwordLen = plaintext.length;
        for (int i = passwordLen; i > MIXCHARS; i -= MIXCHARS) {
            digestA.update(B, 0, MIXCHARS);
        }
        digestA.update(B, 0, passwordLen % MIXCHARS);

        // 步骤11: 处理密码长度二进制位
        for (int i = passwordLen; i > 0; i >>= 1) {
            if ((i & 1) != 0) {
                digestA.update(B, 0, MIXCHARS);
            } else {
                digestA.update(plaintext);
            }
        }

        // 步骤12: 完成Digest A
        byte[] A = digestA.digest();

        // 步骤13-16: 生成字节序列P
        MessageDigest digestDP = MessageDigest.getInstance(digestAlgorithm);
        for (int i = 0; i < passwordLen; i++) {
            digestDP.update(plaintext);
        }
        byte[] DP = digestDP.digest();
        byte[] P = generateByteSequence(DP, passwordLen);

        // 步骤17-20: 生成字节序列S
        int repeatDS = 16 + (A[0] & 0xFF);
        MessageDigest digestDS = MessageDigest.getInstance(digestAlgorithm);
        for (int i = 0; i < repeatDS; i++) {
            digestDS.update(salt);
        }
        byte[] DS = digestDS.digest();
        byte[] S = generateByteSequence(DS, salt.length);

        // 步骤21: 多轮迭代处理
        rounds = (rounds == 0) ? ROUNDS_DEFAULT : rounds;
        byte[] currentDigest = Arrays.copyOf(A, MIXCHARS);
        for (int i = 0; i < rounds; i++) {
            MessageDigest digestC = MessageDigest.getInstance(digestAlgorithm);
            if ((i & 1) != 0) {
                digestC.update(P, 0, passwordLen);
            } else {
                digestC.update(currentDigest, 0, MIXCHARS);
            }

            if (i % 3 != 0) {
                digestC.update(S, 0, salt.length);
            }

            if (i % 7 != 0) {
                digestC.update(P, 0, passwordLen);
            }

            if ((i & 1) != 0) {
                digestC.update(currentDigest, 0, MIXCHARS);
            } else {
                digestC.update(P, 0, passwordLen);
            }

            currentDigest = digestC.digest();
            currentDigest = Arrays.copyOf(currentDigest, MIXCHARS);
        }

        // 构造最终哈希字符串
        return customBase64(currentDigest).getBytes();
    }

    private static byte[] generateByteSequence(byte[] source, int targetLength) {
        byte[] sequence = new byte[targetLength];
        int position = 0;
        while (position < targetLength) {
            int copyLength = Math.min(source.length, targetLength - position);
            System.arraycopy(source, 0, sequence, position, copyLength);
            position += copyLength;
        }
        return sequence;
    }

    private static final char[] B64T =
        "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

    private static String customBase64(byte[] digest) {
        StringBuilder sb = new StringBuilder();

        b64From24bit(sb, digest[0], digest[10], digest[20], 4);
        b64From24bit(sb, digest[21], digest[1], digest[11], 4);
        b64From24bit(sb, digest[12], digest[22], digest[2], 4);
        b64From24bit(sb, digest[3], digest[13], digest[23], 4);
        b64From24bit(sb, digest[24], digest[4], digest[14], 4);
        b64From24bit(sb, digest[15], digest[25], digest[5], 4);
        b64From24bit(sb, digest[6], digest[16], digest[26], 4);
        b64From24bit(sb, digest[27], digest[7], digest[17], 4);
        b64From24bit(sb, digest[18], digest[28], digest[8], 4);
        b64From24bit(sb, digest[9], digest[19], digest[29], 4);
        b64From24bit(sb, 0, digest[31], digest[30], 3); // 最后3字符

        return sb.toString();
    }

    private static void b64From24bit(StringBuilder sb, int b2, int b1, int b0, int n) {
        // 将三个字节组合为24位整数（Java字节是有符号的，需转无符号）
        int w = ((b2 & 0xFF) << 16) | ((b1 & 0xFF) << 8) | (b0 & 0xFF);

        // 按6位分片，映射到自定义字符表
        for (int i = 0; i < n; i++) {
            sb.append(B64T[w & 0x3F]);
            w >>= 6;
        }
    }
}
