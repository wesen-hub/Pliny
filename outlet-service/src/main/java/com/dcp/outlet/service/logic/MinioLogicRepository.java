package com.dcp.outlet.service.logic;

import com.dcp.outlet.adapters.LogicArtifact;
import com.dcp.outlet.adapters.LogicRepository;
import com.dcp.outlet.sdk.LogicRef;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.UUID;

/**
 * M7: 消费端加载的 MinIO 制品仓库({@link LogicRepository} 实现)——替代单机
 * FileLogicRepository, 多实例从共享桶拉取同一份逻辑 jar。
 *
 * <p>流程: HEAD 对象取元数据 sha256 → 本地缓存 {@code <cacheDir>/<sha256>.jar}
 * 命中即返回 → 未命中 GET 到临时文件、校验哈希、原子改名入缓存。
 * 哈希缺/不符 fail-closed(与 FileLogicRepository 同口径)。
 */
public class MinioLogicRepository implements LogicRepository {

    private final S3Client s3;
    private final String bucket;
    private final Path cacheDir;

    public MinioLogicRepository(S3Client s3, String bucket, Path cacheDir) {
        this.s3 = s3;
        this.bucket = bucket;
        this.cacheDir = cacheDir;
    }

    @Override
    public Optional<LogicArtifact> locate(LogicRef ref) {
        String key = MinioArtifactStore.keyOf(ref.logicId(), ref.version());
        HeadObjectResponse head;
        try {
            head = s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (Exception e) {
            throw new IllegalStateException("MinIO 制品定位失败:" + ref, e);
        }
        String sha = head.metadata() == null ? null : head.metadata().get("sha256");
        if (sha == null || sha.isBlank()) {
            throw new IllegalStateException("制品缺 sha256 元数据, 拒绝加载(fail-closed): " + ref
                    + "(旧制品请重新发布)");
        }
        Path cached = ensureCached(ref, key, sha);
        return Optional.of(new LogicArtifact(ref.logicId(), ref.version(), cached, sha));
    }

    /** 缓存命中(按 sha256 命名, 内容寻址); 未命中则下载 + 校验 + 原子入缓存。 */
    private Path ensureCached(LogicRef ref, String key, String sha) {
        Path cached = cacheDir.resolve(sha + ".jar");
        if (Files.isRegularFile(cached)) {
            return cached;
        }
        try {
            Files.createDirectories(cacheDir);
            Path tmp = cacheDir.resolve(sha + ".jar.tmp-" + UUID.randomUUID());
            try {
                ResponseBytes<GetObjectResponse> resp = s3.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(key).build());
                Files.write(tmp, resp.asByteArray());
                String actual = FileLogicRepositoryLike.sha256(tmp);
                if (!actual.equalsIgnoreCase(sha)) {
                    throw new IllegalStateException("MinIO 制品哈希不匹配(拒绝加载): " + ref
                            + " expected=" + sha + " actual=" + actual);
                }
                try {
                    Files.move(tmp, cached, StandardCopyOption.ATOMIC_MOVE);
                } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                    Files.move(tmp, cached);
                }
                return cached;
            } finally {
                try { Files.deleteIfExists(tmp); } catch (IOException ignored) { }
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("MinIO 制品下载失败:" + ref, e);
        }
    }

    /** 极简 sha256(避免为哈希引入 FileLogicRepository 依赖)。 */
    static final class FileLogicRepositoryLike {
        static String sha256(Path file) throws IOException {
            try (java.io.InputStream in = Files.newInputStream(file)) {
                java.security.MessageDigest md;
                try {
                    md = java.security.MessageDigest.getInstance("SHA-256");
                } catch (java.security.NoSuchAlgorithmException e) {
                    throw new IllegalStateException(e);
                }
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) > 0) md.update(buf, 0, n);
                StringBuilder sb = new StringBuilder();
                for (byte b : md.digest()) sb.append(String.format("%02x", b));
                return sb.toString();
            }
        }
    }
}
