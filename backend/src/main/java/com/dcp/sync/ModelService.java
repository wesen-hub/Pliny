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
    private final ShardTableInitializer shardTableInitializer;
    private final ObjectMapper om = new ObjectMapper();

    public ModelService(JdbcTemplate metaJdbc, LogicalTableConfigService tableConfigService,
                        ShardTableInitializer shardTableInitializer) {
        this.metaJdbc = metaJdbc;
        this.tableConfigService = tableConfigService;
        this.shardTableInitializer = shardTableInitializer;
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
     *
     * <p>幂等补完(CP-M 半成品恢复): ① 已落库(此前半成品崩溃/对账重放)时不报错,
     * 加载已有行继续跑 ②③④(全部幂等: upsert / describe-then-create / CREATE IF NOT EXISTS),
     * 收敛到同一目标态; 以已有行为准(模型定义是事实源)。
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
        try {
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
        } catch (org.springframework.dao.DuplicateKeyException dke) {
            // 半成品恢复: 模型已在(此前创建中途崩遇或对账重放) → 加载已有行 → 续跑 ②③④
            long id = existingIdOf(tableName);
            Map<String, Object> row = metaJdbc.queryForMap(
                    "SELECT partition_count, key_columns FROM model_definition WHERE id = ?", id);
            List<String> existingKeys = parseKeys((String) row.get("key_columns"));
            int existingPc = row.get("partition_count") == null
                    ? Math.max(partitionCount, 1) : ((Number) row.get("partition_count")).intValue();
            log.warn("模型已存在, 幂等补完续跑 ②③④: table_name={} id={}", tableName, id);
            publishArtifacts(tableName, topic, existingPc, existingKeys.isEmpty() ? keyColumns : existingKeys);
            return id;
        }
        long id = kh.getKey().longValue();

        publishArtifacts(tableName, topic, Math.max(partitionCount, 1), keyColumns);
        log.info("模型发布: id={} {} → {} topic={} partitions={} tables={} keys={}",
                id, name, tableName, topic, partitionCount, physicalTables, keyColumns);
        return id;
    }

    /** ②③④⑤ 幂等发布物: logical_table upsert + topic 分区确保 + 物理表骨架 + 消费监听(均幂等)。 */
    private void publishArtifacts(String tableName, String topic, int partitionCount, List<String> keyColumns) {
        // ② 逻辑表运行时配置(幂等 upsert; 内部自动建 topic + 分区 + 注册消费监听)
        LogicalTableConfigService.LogicalTableConfig cfg = new LogicalTableConfigService.LogicalTableConfig();
        cfg.setTableName(tableName);
        cfg.setTopic(topic);
        cfg.setBucketCount(4096);
        cfg.setTableCount(1);
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
        shardTableInitializer.ensureShardTables(tableName, 1, keyColumns);
    }

    private long existingIdOf(String tableName) {
        Long id = metaJdbc.queryForObject(
                "SELECT id FROM model_definition WHERE table_name = ?", Long.class, tableName);
        if (id == null) {
            throw new IllegalStateException("模型行存在但查询失败: table_name=" + tableName);
        }
        return id;
    }

    @SuppressWarnings("unchecked")
    private List<String> parseKeys(String keyColumnsJson) {
        if (keyColumnsJson == null || keyColumnsJson.isBlank()) {
            return List.of();
        }
        try {
            return om.readValue(keyColumnsJson, List.class);
        } catch (Exception e) {
            log.warn("model_definition.key_columns JSON 解析失败, 忽略: {}", keyColumnsJson);
            return List.of();
        }
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
