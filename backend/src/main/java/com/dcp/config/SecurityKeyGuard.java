package com.dcp.config;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * 安全密钥启动守卫(H3): 拒绝源码公开的默认密钥, 部署时必须显式提供。
 *
 * <p>application.yml 已去掉兜底默认值(缺失即启动失败); 此处兜底校验长度与已知
 * 默认值, 防 compose/旧配置残留继续使用公开默认值:
 * <ul>
 *   <li>dcp.jwt-secret: HS256 签名要求 ≥32 字节, 且不得等于旧公开默认值;</li>
 *   <li>dcp.enc-key: AES-GCM 数据源密码密钥, 任意长度经 SHA-256 派生(CryptoUtil),
 *       但不得为空/等于旧公开默认值。</li>
 * </ul>
 */
@Component
public class SecurityKeyGuard {

    private static final String DEFAULT_JWT = "dcp-jwt-secret-0123456789abcdef0123456789";
    private static final String DEFAULT_ENC = "dev-enc-key-0123456789abcdef";

    @Value("${dcp.jwt-secret}")
    private String jwtSecret;

    @Value("${dcp.enc-key}")
    private String encKey;

    @PostConstruct
    public void check() {
        byte[] jb = jwtSecret == null ? new byte[0] : jwtSecret.getBytes(StandardCharsets.UTF_8);
        if (jb.length < 32) {
            throw new IllegalStateException("dcp.jwt-secret 未配置或长度不足 32 字节(HS256 最低要求), 请设置环境变量 JWT_SECRET");
        }
        if (DEFAULT_JWT.equals(jwtSecret)) {
            throw new IllegalStateException("dcp.jwt-secret 为源码公开默认值, 拒绝启动; 请设置随机环境变量 JWT_SECRET");
        }
        if (encKey == null || encKey.isBlank()) {
            throw new IllegalStateException("dcp.enc-key 未配置, 请设置环境变量 PLATFORM_ENC_KEY");
        }
        if (DEFAULT_ENC.equals(encKey)) {
            throw new IllegalStateException("dcp.enc-key 为源码公开默认值, 拒绝启动; 请设置随机环境变量 PLATFORM_ENC_KEY");
        }
    }
}
