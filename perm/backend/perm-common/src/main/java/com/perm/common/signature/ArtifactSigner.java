package com.perm.common.signature;

/**
 * 产物签名器（R-Sec6：非对称体系，PDP 持私钥签名，PEP 持公钥验签）
 * 签名输入契约：payload + "&ts=" + ts（payload 为产物序列化，signature/ts 字段置空）。
 */
public interface ArtifactSigner {

    /** 对 payload 签名，返回 Base64 签名串 */
    String sign(String payload);

    /** 签名算法标识（Ed25519 / HmacSHA256），随产物返回供 PEP 选择验签器 */
    String algorithm();
}
