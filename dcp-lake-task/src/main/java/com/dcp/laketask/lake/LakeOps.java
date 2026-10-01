package com.dcp.laketask.lake;

import java.util.List;
import java.util.Map;

/**
 * 湖写端口(读旧行 + 覆盖写, read-modify-write 语义)。
 * 生产实现 {@link IcebergLakeOps}(OverwriteFiles.overwriteByRowFilter); 单测以 fake 实现注入。
 *
 * <p>方法均为批级原子语义: rewrite 一次 commit(防半写); 失败抛异常,
 * 底座据此不提交 offset → 重投 → 死信(at-least-once 幂等)。
 */
public interface LakeOps extends AutoCloseable {

    /**
     * 确保湖表存在(不存在以键列/字段列/seq 列建表) + 缺列补列(schema evolution,
     * 与批作业 dcp-lakesync ensureColumns 同向: 字段列 string, seq·写者列 long)。
     */
    void ensureTable(List<String> keyColumns, List<String> fieldCols, List<String> seqCols);

    /**
     * 读指定业务键的现行行(与 {@code touchKeys} 顺序对齐; 无行 → null)。
     * {@code touchKeys} 每项 = 键值数组(与 {@code keyColumns} 顺序对齐)。
     */
    List<Map<String, String>> loadRows(List<String> keyColumns, List<List<String>> touchKeys);

    /**
     * 整批覆盖提交: {@code touchKeys} 全部业务键的匹配行先删, 再写回
     * {@code upsertRows}(与 {@code touchKeys} 同序, null = 只删不写)。
     * 等价 Spark MERGE 的 UPDATE+INSERT, 湖表保持"每业务键一行"权威态。
     */
    void rewrite(List<String> keyColumns, List<List<String>> touchKeys, List<Map<String, String>> upsertRows);

    @Override
    void close();
}
