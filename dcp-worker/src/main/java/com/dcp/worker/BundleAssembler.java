package com.dcp.worker;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * 业务 jar 装配器(方案 §8.2 装配流程的第 1-3 步):
 * worker.bundles 配置(ns@jar:version)→ MinIO jar_release 拉取 → sha256 校验
 * → 白名单 ClassLoader 装载。本地文件路径条目直接装载(联调兼容)。
 *
 * <pre>
 *   bundles 条目语法(逗号分隔):
 *     "sync@dcp-synclink:0.1.0"   → MinIO bucket=jar_release, 对象键 {artifactId}/{version}.jar
 *                                   (+ 同名 .sha256 sidecar 校验)
 *     "sync@/opt/bundles/x.jar"   → 本地文件(联调/宿主机构建产物)
 * </pre>
 */
@Component
public class BundleAssembler {

    private static final Logger log = LoggerFactory.getLogger(BundleAssembler.class);

    @Value("${dcp.worker.bundles:}")
    private String bundles;

    @Value("${dcp.worker.minio.endpoint:http://127.0.0.1:9000}")
    private String minioEndpoint;
    @Value("${dcp.worker.minio.access-key:}")
    private String minioAccessKey;
    @Value("${dcp.worker.minio.secret-key:}")
    private String minioSecretKey;
    @Value("${dcp.worker.minio.bucket:jar-release}")
    private String minioBucket;
    @Value("${dcp.worker.bundle-cache:./bundles}")
    private String bundleCache;

    private MinioClient minio;

    /** 装载全部 bundle,返回白名单 ClassLoader 列表(骨架停机时统一关闭)。 */
    public List<WhitelistClassLoader> assemble() {
        List<WhitelistClassLoader> loaders = new ArrayList<>();
        if (bundles == null || bundles.isBlank()) {
            log.warn("[worker] 未配置 dcp.worker.bundles, 无业务执行器装载");
            return loaders;
        }
        for (String entry : bundles.split(",")) {
            String e = entry.trim();
            if (e.isEmpty()) continue;
            try {
                Path jar = resolve(e);
                WhitelistClassLoader cl = new WhitelistClassLoader(
                        new URL[]{jar.toUri().toURL()},
                        WorkerRuntime.class.getClassLoader());
                loaders.add(cl);
                log.info("[worker] 业务 jar 装载: {} ← {}", e, jar);
            } catch (Exception ex) {
                log.error("[worker] 业务 jar 装载失败, 跳过: {} - {}", e, ex.getMessage());
            }
        }
        return loaders;
    }

    /** 解析 bundle 条目为本地 jar 路径(MinIO 拉取或本地直取)。 */
    private Path resolve(String entry) throws Exception {
        // 含 "/" 视为本地路径(联调),否则按 ns@artifactId:version 从 MinIO 拉取
        int at = entry.indexOf('@');
        String ref = at >= 0 ? entry.substring(at + 1) : entry;
        if (ref.contains("/")) {
            Path p = Path.of(ref);
            if (!Files.isRegularFile(p)) {
                throw new IllegalStateException("本地业务 jar 不存在: " + p);
            }
            return p;
        }
        String[] parts = ref.split(":");
        if (parts.length != 2) {
            throw new IllegalStateException("bundle 条目语法错误(应为 ns@artifactId:version 或 ns@/path): " + entry);
        }
        String artifactId = parts[0];
        String version = parts[1];
        String objectKey = artifactId + "/" + version + ".jar";
        String shaKey = objectKey + ".sha256";
        Path target = Path.of(bundleCache, artifactId + "-" + version + ".jar");
        try (InputStream in = minioClient().getObject(GetObjectArgs.builder()
                .bucket(minioBucket).object(objectKey).build())) {
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        }
        // sha256 校验(fail-closed:哈希不符/缺失拒绝装载,与逻辑仓库 JarLogicLoader 同语义)
        String expected = readSidecar(minioBucket, shaKey);
        String actual = sha256(target);
        if (!actual.equalsIgnoreCase(expected)) {
            throw new IllegalStateException("业务 jar 哈希不匹配(拒绝装载):" + entry
                    + " expected=" + expected + " actual=" + actual);
        }
        return target;
    }

    private String readSidecar(String bucket, String key) throws Exception {
        try (InputStream in = minioClient().getObject(GetObjectArgs.builder()
                .bucket(bucket).object(key).build())) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
    }

    private MinioClient minioClient() {
        if (minio == null) {
            minio = MinioClient.builder()
                    .endpoint(minioEndpoint)
                    .credentials(minioAccessKey, minioSecretKey)
                    .build();
        }
        return minio;
    }

    private static String sha256(Path file) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] digest = md.digest(Files.readAllBytes(file));
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
