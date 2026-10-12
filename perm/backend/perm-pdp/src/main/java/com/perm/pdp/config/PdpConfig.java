package com.perm.pdp.config;

import com.perm.common.compile.PolicyCompiler;
import com.perm.common.signature.ArtifactSigner;
import com.perm.common.signature.ArtifactVerifier;
import com.perm.common.signature.Ed25519Keys;
import com.perm.common.signature.Ed25519Signer;
import com.perm.common.signature.Ed25519Verifier;
import com.perm.pdp.cache.PlanCache;
import com.perm.pdp.policy.PolicySnapshotLoader;
import com.perm.pdp.sync.RestSnapshotFetcher;
import com.perm.pdp.sync.SnapshotFetcher;
import com.perm.pdp.sync.SnapshotSyncService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

/**
 * PDP Bean 装配（perm-common 无 Spring 依赖，由本服务装配其组件）
 * 快照同步链路：SnapshotSyncService + RestSnapshotFetcher（定时任务驱动）
 */
@Configuration
public class PdpConfig {

    @Bean
    public PolicyCompiler policyCompiler() {
        return new PolicyCompiler();
    }

    /** 产物签名器（R-Sec6：Ed25519 私钥签名，私钥 Base64 注入；正式形态 KMS 管理 + 轮换） */
    @Bean
    public ArtifactSigner artifactSigner(@Value("${perm.security.ed25519-private-key}") String privateKeyBase64) {
        return new Ed25519Signer(Ed25519Keys.parsePrivate(privateKeyBase64));
    }

    /** 产物验签器（管理面自检 / PEP 公钥验签参考；演示私钥配套公钥） */
    @Bean
    public ArtifactVerifier artifactVerifier(@Value("${perm.security.ed25519-public-key}") String publicKeyBase64) {
        return new Ed25519Verifier(Ed25519Keys.parsePublic(publicKeyBase64));
    }

    @Bean
    public RestClient restClient() {
        return RestClient.create();
    }

    @Bean
    public SnapshotFetcher snapshotFetcher(RestClient restClient,
                                          @Value("${perm.admin.url:http://localhost:8082}") String adminUrl) {
        return new RestSnapshotFetcher(restClient, adminUrl);
    }

    @Bean
    public SnapshotSyncService snapshotSyncService(PolicySnapshotLoader snapshotLoader,
                                                   PlanCache planCache,
                                                   PolicyCompiler compiler,
                                                   SnapshotFetcher fetcher) {
        return new SnapshotSyncService(snapshotLoader, planCache, compiler, fetcher);
    }
}
