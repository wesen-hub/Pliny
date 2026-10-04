package com.dcp.sync;

import com.dcp.module.datasource.entity.DatasourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 快照起点位点读取器(方案 A, 设计文档 §2.2 实测定案 2026-10-05)。
 *
 * <p>背景: 实测 MySQL 快照行位点列为空形态("mysql::0:", file=""/pos=0, 无 snapshot 键),
 * 无法从行内取起点编码快照级号 S(方案 B 实质不成立)。本类在任务提交前读源库
 * <b>当前位点</b>下发为 snapshot_start_position, 并持久化到 cdc_st_task 保证重放时 S 恒定;
 * S &lt; 一切流号(增量位点从启动时刻继续增长), 快照→增量衔接不等式成立。
 *
 * <p>位点字符串格式与补丁层 PositionMetadataConverter 透传格式一致("type:k1:v1:..."):
 * <pre>
 *   mysql      SHOW MASTER STATUS → "mysql:{file}:{pos}:0"
 *   pg         pg_current_wal_lsn()::text → "pg:{lsn}"
 *   oracle     SELECT CURRENT_SCN FROM V$DATABASE → "oracle:{scn}:0"
 * </pre>
 * 其余源(sqlserver/mongodb/tidb)当前位点读取 TODO(实跑确认后补);
 * 未实现源 fail-fast(快照行无 S 无法编码, 不允许静默降级)。
 */
@Slf4j
@Component
public class CurrentPositionReader {

    /** 读取源库当前位点; 读失败/未实现源 → fail-fast(快照行无 S 无法编码) */
    public String read(String dsType, DatasourceConfig ds, String password, String sourceDb) {
        String type = dsType == null ? "" : dsType.toLowerCase();
        String url;
        switch (type) {
            case "mysql" -> url = "jdbc:mysql://%s:%d?useSSL=false&allowPublicKeyRetrieval=true"
                    .formatted(ds.getHost(), ds.getPort());
            case "postgresql" -> url = "jdbc:postgresql://%s:%d/%s"
                    .formatted(ds.getHost(), ds.getPort(), sourceDb);
            case "oracle" -> url = "jdbc:oracle:thin:@//%s:%d/%s"
                    .formatted(ds.getHost(), ds.getPort(),
                            ds.getDatabaseName() == null || ds.getDatabaseName().isBlank()
                                    ? sourceDb : ds.getDatabaseName());
            default -> throw new IllegalStateException("快照起点位点读取未实现源类型: " + type
                    + "(快照行无 S 无法编码, fail-fast; TODO: sqlserver/mongodb/tidb 实跑确认后补)");
        }
        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), password);
             Statement st = conn.createStatement()) {
            switch (type) {
                case "mysql" -> {
                    try (ResultSet rs = st.executeQuery("SHOW MASTER STATUS")) {
                        if (rs.next()) {
                            String file = rs.getString("File");
                            long pos = rs.getLong("Position");
                            return "mysql:" + file + ":" + pos + ":0";
                        }
                        throw new IllegalStateException("SHOW MASTER STATUS 无结果(binlog 未开启?)");
                    }
                }
                case "postgresql" -> {
                    try (ResultSet rs = st.executeQuery("SELECT pg_current_wal_lsn()::text")) {
                        if (rs.next()) {
                            return "pg:" + rs.getString(1);
                        }
                        throw new IllegalStateException("pg_current_wal_lsn() 无结果");
                    }
                }
                case "oracle" -> {
                    try (ResultSet rs = st.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE")) {
                        if (rs.next()) {
                            return "oracle:" + rs.getLong(1) + ":0";
                        }
                        throw new IllegalStateException("SELECT CURRENT_SCN 无结果");
                    }
                }
                default -> throw new IllegalStateException("未实现源类型: " + type);
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("读源库当前位点失败(" + type + "): " + e.getMessage(), e);
        }
    }
}
