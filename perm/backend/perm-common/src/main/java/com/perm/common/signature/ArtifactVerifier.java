package com.perm.common.signature;

/**
 * 产物验签器（R-Sec6：PEP 侧持公钥验签；作业镜像可内嵌公钥）
 */
public interface ArtifactVerifier {

    /** 验签，签名非法/篡改返回 false（常量时间比对） */
    boolean verify(String payload, String signature);

    /** 支持的签名算法标识 */
    String algorithm();
}
