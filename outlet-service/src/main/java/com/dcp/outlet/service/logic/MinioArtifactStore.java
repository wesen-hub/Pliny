package com.dcp.outlet.service.logic;

import com.dcp.outlet.control.LogicPublishException;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.Map;

/**
 * MinIO 共享桶制品仓库(M7): key = {@code logic/{logicId}/{version}.jar}。
 *
 * <p>原子性: 单次 PutObject 即原子落地(无半成品可见窗口, 天然免临时名改名);
 * jar 与 sha256 在**同一对象**内(哈希内嵌为对象元数据 x-amz-meta-sha256),
 * 不存在 jar/sidecar 两步不一致; 桶开启版本化后覆盖可追溯。
 */
public class MinioArtifactStore implements LogicArtifactStore {

    private static final String META_SHA256 = "sha256";
    private static final String META_LOGIC_ID = "logic-id";
    private static final String META_VERSION = "version";

    private final S3Client s3;
    private final String bucket;

    public MinioArtifactStore(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    static String keyOf(String logicId, String version) {
        return "logic/" + logicId + "/" + version + ".jar";
    }

    @Override
    public void putArtifact(String logicId, String version, byte[] jar, String sha256) {
        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucket)
                .key(keyOf(logicId, version))
                .metadata(Map.of(
                        META_SHA256, sha256,
                        META_LOGIC_ID, logicId,
                        META_VERSION, version))
                .build();
        try {
            s3.putObject(req, RequestBody.fromBytes(jar));
        } catch (Exception e) {
            throw new LogicPublishException("写 MinIO 仓库失败:" + logicId + "@" + version, e);
        }
    }

    @Override
    public byte[] getArtifact(String logicId, String version) {
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucket)
                .key(keyOf(logicId, version))
                .build();
        try {
            ResponseBytes<GetObjectResponse> resp = s3.getObjectAsBytes(req);
            return resp.asByteArray();
        } catch (Exception e) {
            throw new LogicPublishException("读 MinIO 仓库失败:" + logicId + "@" + version, e);
        }
    }

    @Override
    public void deleteArtifact(String logicId, String version) {
        try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(keyOf(logicId, version))
                    .build());
        } catch (Exception ignored) {
            // 回滚路径尽力而为; 版本化桶下误删亦有版本可追
        }
    }

    @Override
    public String describe(String logicId, String version) {
        return "minio://" + bucket + "/" + keyOf(logicId, version);
    }
}
