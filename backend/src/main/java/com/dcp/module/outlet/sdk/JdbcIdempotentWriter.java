package com.dcp.module.outlet.sdk;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 幂等写入的 JDBC 实现:ODKU + {@code _commit_ts} 版本收敛。
 *
 * <p>生成形如:
 * <pre>
 * INSERT INTO t (id, name, _commit_ts) VALUES (?, ?, ?)
 * ON DUPLICATE KEY UPDATE
 *   name = IF(VALUES(_commit_ts) &gt; _commit_ts, VALUES(name), name),
 *   _commit_ts = IF(VALUES(_commit_ts) &gt; _commit_ts, VALUES(_commit_ts), _commit_ts)
 * </pre>
 * 前提:目标表有 {@code _commit_ts BIGINT}(DDL 见《表结构DDL》§7)。
 */
@Slf4j
@RequiredArgsConstructor
public class JdbcIdempotentWriter implements IdempotentWriter {

    public static final String VERSION_COLUMN = "_commit_ts";

    private final JdbcTemplate jdbc;
    private final String pkColumn;

    @Override
    public void upsert(String table, Map<String, Object> row, long commitTs) {
        Map<String, Object> data = new LinkedHashMap<>(row);
        data.put(VERSION_COLUMN, commitTs);
        String cols = String.join(", ", data.keySet());
        String marks = data.keySet().stream().map(c -> "?").collect(Collectors.joining(", "));
        String updates = data.keySet().stream()
                .filter(c -> !pkColumn.equals(c))
                .map(c -> c + " = IF(VALUES(" + VERSION_COLUMN + ") >= " + VERSION_COLUMN
                        + ", VALUES(" + c + "), " + c + ")")
                .collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + table + " (" + cols + ") VALUES (" + marks + ") "
                + "ON DUPLICATE KEY UPDATE " + updates;
        jdbc.update(sql, data.values().toArray());
    }

    @Override
    public void delete(String table, Map<String, Object> key, long commitTs) {
        // 墓碑:物理删（若目标表需要保留删除痕迹,可改为标记列;此处按"数据面权威"口径物理删）
        jdbc.update("DELETE FROM " + table + " WHERE " + pkColumn + " = ?", key.get(pkColumn));
    }
}
