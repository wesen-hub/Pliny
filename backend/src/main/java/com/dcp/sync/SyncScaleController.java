package com.dcp.sync;

import com.dcp.common.ApiResponse;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 扩容 API(V3 §6):
 *
 * <pre>
 *   GET  /api/sync/scale/lag/{table}              单逻辑表 lag(各分区 + 总量)
 *   POST /api/sync/scale/expand/{table}?partitions=16  加分区扩容(建分区 + 重算桶映射 + 热更新)
 *   POST /api/sync/scale/migrate/{table}          自定义桶迁移 body {targetPartition, buckets[]}(热点桶优先搬新分区)
 *   POST /api/sync/scale/migrate-hot/{table}?targetPartition=&topN=  热点拆分建议(分区 lag 排行 + 确定性抽样桶迁移)
 *   GET  /api/sync/scale/bucket-map?topic=        当前桶映射全量 JSON(topic 反查; 生产侧 BucketMapPartitioner 查询端点, 60s TTL)
 * </pre>
 */
@RestController
@RequestMapping("/api/sync/scale")
public class SyncScaleController {

    private final ScaleService scaleService;
    private final BucketMapService bucketMapService;
    private final JdbcTemplate metaJdbc;

    public SyncScaleController(ScaleService scaleService, BucketMapService bucketMapService,
                               JdbcTemplate metaJdbc) {
        this.scaleService = scaleService;
        this.bucketMapService = bucketMapService;
        this.metaJdbc = metaJdbc;
    }

    @GetMapping("/lag/{table}")
    public ApiResponse<Map<String, Object>> lag(@PathVariable String table) {
        return ApiResponse.ok(scaleService.lagOf(table));
    }

    @PostMapping("/expand/{table}")
    public ApiResponse<Map<String, Object>> expand(@PathVariable String table,
                                      @RequestParam int partitions) {
        return ApiResponse.ok(scaleService.expandPartitions(table, partitions));
    }

    /** 自定义桶迁移: body {targetPartition: int, buckets: [int...]} */
    @PostMapping("/migrate/{table}")
    public ApiResponse<Map<String, Object>> migrate(@PathVariable String table,
                                                    @RequestBody Map<String, Object> body) {
        int targetPartition = ((Number) body.get("targetPartition")).intValue();
        @SuppressWarnings("unchecked")
        List<Integer> buckets = (List<Integer>) body.get("buckets");
        return ApiResponse.ok(bucketMapService.migrateBuckets(table, targetPartition, buckets));
    }

    /** 热点拆分建议: 分区 lag 排行取热点分区 → 确定性抽样桶 → 迁往目标分区 */
    @PostMapping("/migrate-hot/{table}")
    public ApiResponse<Map<String, Object>> migrateHot(@PathVariable String table,
                                                       @RequestParam int targetPartition,
                                                       @RequestParam(defaultValue = "128") int topN) {
        return ApiResponse.ok(scaleService.suggestMigration(table, targetPartition, topN));
    }

    /**
     * 当前桶映射全量 JSON(topic 反查逻辑表; 生产侧 Partitioner 60s TTL 查询, 桶号 → 分区号)。
     * topic 无对应逻辑表配置 → 返回空映射(Partitioner 回退默认分区器, 与未接入行为一致)。
     */
    @GetMapping("/bucket-map")
    public ApiResponse<Map<String, Object>> bucketMapByTopic(@RequestParam String topic) {
        List<String> tables = metaJdbc.queryForList(
                "SELECT table_name FROM logical_table WHERE topic = ?", String.class, topic);
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("topic", topic);
        out.put("logicalTable", tables.isEmpty() ? null : tables.get(0));
        out.put("bucketMap", tables.isEmpty()
                ? java.util.Collections.emptyMap() : bucketMapService.bucketMapOf(tables.get(0)));
        return ApiResponse.ok(out);
    }
}
