package com.perm.common.signature;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.security.KeyFactory;
import java.util.Base64;

/**
 * Ed25519 密钥工具（R-Sec6/R-Sec8：密钥生成与解析）
 * 演示级：密钥对 Base64 编码文本承载；正式形态由 KMS 管理 + 轮换。
 * CLI 用法：java -cp perm-common.jar com.perm.common.signature.Ed25519Keys generate
 */
public final class Ed25519Keys {

    private Ed25519Keys() {
    }

    /** 生成密钥对 */
    public static KeyPair generate() {
        try {
            return KeyPairGenerator.getInstance(Ed25519Signer.ALGORITHM).generateKeyPair();
        } catch (Exception e) {
            throw new IllegalStateException("Ed25519 密钥对生成失败", e);
        }
    }

    /** 私钥 → Base64（PKCS#8） */
    public static String encodePrivate(PrivateKey key) {
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }

    /** 公钥 → Base64（X.509） */
    public static String encodePublic(PublicKey key) {
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }

    /** Base64 → 私钥 */
    public static PrivateKey parsePrivate(String base64) {
        try {
            return KeyFactory.getInstance(Ed25519Signer.ALGORITHM)
                    .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(base64)));
        } catch (Exception e) {
            throw new IllegalArgumentException("私钥解析失败", e);
        }
    }

    /** Base64 → 公钥 */
    public static PublicKey parsePublic(String base64) {
        try {
            return KeyFactory.getInstance(Ed25519Signer.ALGORITHM)
                    .generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(base64)));
        } catch (Exception e) {
            throw new IllegalArgumentException("公钥解析失败", e);
        }
    }

    /** CLI：生成演示密钥对（打印 Base64 私钥/公钥） */
    public static void main(String[] args) {
        KeyPair kp = generate();
        System.out.println("PRIVATE=" + encodePrivate(kp.getPrivate()));
        System.out.println("PUBLIC=" + encodePublic(kp.getPublic()));
    }
}
