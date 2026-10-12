package com.perm.common.signature;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;

/**
 * Ed25519 验签器（PEP/SDK 侧，R-Sec6）
 * 公钥 Base64 编码注入（作业镜像可内嵌公钥）。
 */
public class Ed25519Verifier implements ArtifactVerifier {

    public static final String ALGORITHM = "Ed25519";

    private final PublicKey publicKey;

    public Ed25519Verifier(PublicKey publicKey) {
        this.publicKey = publicKey;
    }

    @Override
    public boolean verify(String payload, String signature) {
        if (signature == null) {
            return false;
        }
        try {
            Signature sig = Signature.getInstance(ALGORITHM);
            sig.initVerify(publicKey);
            sig.update(payload.getBytes(StandardCharsets.UTF_8));
            return sig.verify(Base64.getDecoder().decode(signature));
        } catch (Exception e) {
            return false; // 签名非法/篡改 → 一律不通过（fail-closed）
        }
    }

    @Override
    public String algorithm() {
        return ALGORITHM;
    }
}
