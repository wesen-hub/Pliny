package com.dcp.outlet.service.logic;

/**
 * M7: 逻辑制品仓库端口——发布/下载/回滚的存储抽象。
 *
 * <p>生产实现 {@link MinioArtifactStore}(共享桶, PutObject 原子 + 桶版本化);
 * 本地骨架 {@link FileArtifactStore}(目录布局 {@code <root>/<logicId>/<version>.jar},
 * 临时名→sidecar→原子改名, 用于离线测试/单机退化)。
 *
 * <p>所有实现必须保证: 成功返回后制品完整可见; {@code sha256} 与 jar 同落(MinIO 走
 * 对象元数据内嵌哈希, 天然原子; 本地走 sidecar 文件, 先 sidecar 后 jar 保证 fail-closed)。
 */
public interface LogicArtifactStore {

    /** 写入制品(同版本禁止覆盖, 由调用方先查 DB 冲突; 实现仍需保证原子可见)。 */
    void putArtifact(String logicId, String version, byte[] jar, String sha256);

    /** 读取制品字节。 */
    byte[] getArtifact(String logicId, String version);

    /** 删除制品(登记回滚用; 幂等)。 */
    void deleteArtifact(String logicId, String version);

    /** 制品 URI 描述(登记 artifactUri 用)。 */
    String describe(String logicId, String version);
}
