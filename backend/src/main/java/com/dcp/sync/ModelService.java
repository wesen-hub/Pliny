package com.dcp.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

/**
 * 模型管理(建模即发布): 创建模型 = 定义逻辑表 + 建 topic/分区 + 预建物理表 + 记录元信息。
 * 同步任务向导选模型 → 任务走模型 topic/分区、写模型物理表(分表组)。
 */
@Slf4j
@Service
public class ModelService {

    private final JdbcTemplate metaJdbc;
    private final LogicalTableConfigService tableConfigService;
    private final TargetDbRegistry targetDbRegistry;
    private final ObjectMapper om = new ObjectMapper();

    public ModelService(JdbcTemplate metaJdbc, LogicalTableConfigService tableConfigService,
                        TargetDbRegistry targetDbRegistry) {
        this.metaJdbc = metaJdbc;
        this.tableConfigService = tableConfigService;
        this.targetDbRegistry = targetDbRegistry;
    }

    public List<Map<String, Object>> list() {
        return metaJdbc.queryForList(
                "SELECT id, name, layer, domain, table_name, topic, partition_count, table_count, "
                        + "key_columns, fields, description, create_time FROM model_definition ORDER BY id DESC");
    }

    public Map<String, Object> get(long id) {
        try {
            return metaJdbc.queryForMap(
                    "SELECT id, name, layer, domain, table_name, topic, partition_count, table_count, "
                            + "key_columns, fields, description, create_time FROM model_definition WHERE id = ?", id);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 创建模型(发布):
     * ① 写 model_definition; ② upsert logical_table(运行时配置: topic/分区/表数/键);
     * ③ 自动建 topic + Kafka 分区; ④ 预建物理表骨架; ⑤ 注册消费监听
     */
    public long create(String name, String layer, String domain, String tableName,
                       int partitionCount, int tableCount, List<String> keyColumns,
                       String fieldsJson, String description) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("逻辑表名不能为空");
        }
        if (keyColumns == null || keyColumns.isEmpty()) {
            throw new IllegalArgumentException("请指定业务键字段(目标侧路由/收敛键)");
        }
        // 去分片(2026-09-07): 物理单表 = 逻辑表(TiDB 自动 split), 参数 tableCount 不再生效
        final int physicalTables = 1;
        String topic = "dcp." + tableName;
        // ① 模型定义
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO model_definition (name, layer, domain, table_name, topic, bucket_count, "
                            + "partition_count, table_count, key_columns, fields, description) VALUES (?, ?, ?, ?, ?, 4096, ?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name == null || name.isBlank() ? tableName : name);
            ps.setString(2, layer == null ? "DWD" : layer);
            ps.setString(3, domain);
            ps.setString(4, tableName);
            ps.setString(5, topic);
            ps.setInt(6, Math.max(partitionCount, 1));
            ps.setInt(7, physicalTables);
            ps.setString(8, toJson(keyColumns));
            ps.setString(9, fieldsJson);
            ps.setString(10, description);
            return ps;
        }, kh);
        long id = kh.getKey().longValue();

        // ② 逻辑表运行时配置(幂等 upsert; 内部自动建 topic + 分区 + 注册消费监听)
        LogicalTableConfigService.LogicalTableConfig cfg = new LogicalTableConfigService.LogicalTableConfig();
        cfg.setTableName(tableName);
        cfg.setTopic(topic);
        cfg.setBucketCount(4096);
        cfg.setTableCount(physicalTables);
        cfg.setTablePrefix(tableName);
        cfg.setPartitionCount(Math.max(partitionCount, 1));
        cfg.setConsumerGroup("dcp-sync-" + tableName);
        Map<String, Object> bk = new LinkedHashMap<>();
        bk.put("mode", "merge");
        bk.put("keyFields", keyColumns);
        bk.put("scopeFields", List.of());
        bk.put("systemNames", List.of());
        cfg.setBusinessKey(bk);
        cfg.setKeyColumns(keyColumns);
        tableConfigService.save(cfg);   // ③⑤ 自动: ensureTopicPartitions + registry.register

        // ④ 预建物理表骨架(target_db; 多库场景模型默认写 target_db)
        targetDbRegistry.ensureShardTables(tableName, physicalTables, keyColumns);
        log.info("模型发布: id={} {} → {} topic={} partitions={} tables={} keys={}",
                id, name, tableName, topic, partitionCount, physicalTables, keyColumns);
        return id;
    }

    public void delete(long id) {
        metaJdbc.update("DELETE FROM model_definition WHERE id = ?", id);
    }

    private String toJson(Object o) {
        try {
            return om.writeValueAsString(o);
        } catch (Exception e) {
            return "[]";
        }
    }
}
