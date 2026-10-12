package com.perm.common.signature;

import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Base64;

/**
 * Ed25519 签名器（PDP 侧，R-Sec6）
 * JDK 15+ 内置支持，无外部依赖；私钥 Base64 编码注入。
 */
public class Ed25519Signer implements ArtifactSigner {

    public static final String ALGORITHM = "Ed25519";

    private final PrivateKey privateKey;

    public Ed25519Signer(PrivateKey privateKey) {
        this.privateKey = privateKey;
    }

    @Override
    public String sign(String payload) {
        try {
            Signature sig = Signature.getInstance(ALGORITHM);
            sig.initSign(privateKey);
            sig.update(payload.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(sig.sign());
        } catch (Exception e) {
            throw new IllegalStateException("Ed25519 签名失败", e);
        }
    }

    @Override
    public String algorithm() {
        return ALGORITHM;
    }
}
