package com.dcp.filesync.util;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * AES-256-GCM 解密(与 backend com.dcp.common.CryptoUtil 同构的独立复制, fat jar 自足模式;
 * 方案 N4 先例: 契约外工具以业务包名独立复制, backend 原类不动)。
 *
 * <p>密文格式: base64(IV(12B) || Ciphertext || Tag(16B)); 密钥任意长度 → SHA-256 派生 32 字节。
 * 业务 jar 只读凭证(执行态), 无加密需求, 故仅提供 decrypt。
 */
public final class CryptoUtil {

    private static final int IV_LEN = 12;
    private static final int TAG_BITS = 128;

    private CryptoUtil() {
    }

    public static String decrypt(String cipherText, String key) {
        try {
            byte[] all = Base64.getDecoder().decode(cipherText);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(keyBytes(key), "AES"),
                    new GCMParameterSpec(TAG_BITS, all, 0, IV_LEN));
            byte[] pt = cipher.doFinal(all, IV_LEN, all.length - IV_LEN);
            return new String(pt, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("AES 解密失败", e);
        }
    }

    private static byte[] keyBytes(String key) {
        try {
            var md = java.security.MessageDigest.getInstance("SHA-256");
            return md.digest(key.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
