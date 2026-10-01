package com.dcp.synclink.core;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 逻辑表配置读取器(业务 jar 自足): 从元库 logical_table 加载写路径所需的分表规则。
 *
 * <p>与 backend LogicalTableConfigService 无共享(控制面保存/提交 TaskSpec 职责留在宿主侧);
 * 本类只读 + 缓存, 未配置时返回默认(演示用, 与旧链路语义一致)。
 */
@Slf4j
public class LogicalTableReader {

    private final JdbcTemplate metaJdbc;
    private final java.util.Map<String, LogicalTableConfig> cache = new ConcurrentHashMap<>();

    public LogicalTableReader(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    /** 加载(带缓存); 未配置时返回默认配置并告警日志 */
    public LogicalTableConfig load(String logicalTable) {
        LogicalTableConfig cached = cache.get(logicalTable);
        if (cached != null) {
            return cached;
        }
        LogicalTableConfig cfg = new LogicalTableConfig();
        cfg.setTableName(logicalTable);
        try {
            metaJdbc.query("SELECT table_count, table_prefix FROM logical_table WHERE table_name = ?",
                    rs -> {
                        cfg.setTableCount(rs.getInt("table_count"));
                        cfg.setTablePrefix(rs.getString("table_prefix"));
                    }, logicalTable);
        } catch (Exception e) {
            log.warn("逻辑表 {} 未在元库配置, 使用默认分表规则(prefix=t_order, count=50)", logicalTable);
            cfg.setTablePrefix("t_order");
        }
        cache.put(logicalTable, cfg);
        return cfg;
    }

    /** 配置变更后清缓存(逻辑表重建/修改由宿主侧触发) */
    public void invalidate(String logicalTable) {
        cache.remove(logicalTable);
    }
}
