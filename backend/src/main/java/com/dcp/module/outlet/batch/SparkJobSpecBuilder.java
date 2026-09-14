package com.dcp.module.outlet.batch;

import com.dcp.module.outlet.OutletProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Spark 作业规格生成器(设计 §6.3 技术要点)。
 *
 * <p>把 §6.3 的六条"必须做到"编码进规格里,避免实现时漏项:
 * <ol>
 *   <li><b>T0 下发到每个并发连接</b>:SQL 用 `AS OF TIMESTAMP '<T0>'`,并带 {@code sessionInitStatement}(会话级兜底);</li>
 *   <li><b>GC 校验</b>:{@code gcCheck=true} + {@code earliestTs},作业端拒绝越界;</li>
 *   <li><b>权限不绕</b>:只给出口 L3 读入口({@code readEntry}),不给数据面裸库连接串;</li>
 *   <li><b>原子可见</b>:{@code staging} 按 boundary/区间版本化,{@code manifestRequired=true};</li>
 *   <li><b>位点收口</b>:{@code commitAfterPublish=true}(由 BatchJobService 在发布后提交);</li>
 *   <li><b>资源边界</b>:{@code resourceProfile}(executor 核数/内存/队列)。</li>
 * </ol>
 */
@RequiredArgsConstructor
public class SparkJobSpecBuilder {

    private final OutletProperties props;
    private final ObjectMapper json = new ObjectMapper();

    public SparkJobSpec build(BatchJobSpec spec, String mode, Long boundaryTso, Long loTso, Long hiTso) {
        String range = boundaryTso != null ? String.valueOf(boundaryTso) : (loTso + "-" + hiTso);
        String staging = stagingRoot() + "/" + spec.getConsumerId() + "/" + spec.getStreamId() + "/" + mode + "/" + range;

        Map<String, Object> job = new LinkedHashMap<>();
        job.put("jobName", "outlet-batch-" + spec.getStreamId() + "-" + mode);
        job.put("mode", mode);
        job.put("stream", spec.getStreamId());
        if (boundaryTso != null) {
            job.put("boundaryTso", boundaryTso);
            job.put("snapshotSql", "SELECT * FROM " + spec.getStreamId()
                    + " AS OF TIMESTAMP '" + toDatetime(boundaryTso) + "' WHERE " + pkColumn() + " > ? ORDER BY "
                    + pkColumn() + " LIMIT " + spec.getSnapshotLimit());
            job.put("sessionInitStatement", "SET @@tidb_snapshot='" + boundaryTso + "'");
        } else {
            job.put("loTso", loTso);
            job.put("hiTso", hiTso);
            job.put("changesQuery", "GET /api/chg/" + spec.getStreamId() + "/changes?after={position}&limit=" + spec.getBatchLimit());
        }
        job.put("readEntry", readEntry());
        job.put("readViaContract", true);              // ★ 权限不绕:只经出口读入口
        job.put("gcCheck", true);                      // ★ 越界即拒
        job.put("contractor", spec.getConsumerId());
        job.put("transform", spec.getTransform());
        job.put("target", spec.getTarget());
        job.put("outputFormat", "iceberg|parquet");
        job.put("staging", staging);                   // ★ 不可变、版本化
        job.put("manifestRequired", true);             // ★ 发布后才可见
        job.put("commitAfterPublish", true);           // ★ 位点收口
        job.put("resourceProfile", resourceProfile());

        String jobJson;
        try {
            jobJson = json.writeValueAsString(job);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("生成 Spark 作业描述失败", e);
        }

        List<String> args = new ArrayList<>();
        args.add(props.getSpark().getSparkSubmit());
        args.add("--master");
        args.add(props.getSpark().getMaster());
        args.add("--class");
        args.add("com.dcp.spark.OutletBatchJob");
        args.add("--conf");
        args.add("spark.sql.session.timeZone=Asia/Shanghai");
        args.add("--conf");
        args.add("spark.executor.cores=" + props.getSpark().getExecutorCores());
        args.add("--conf");
        args.add("spark.executor.memory=" + props.getSpark().getExecutorMemory());
        args.add(props.getSpark().getJobJar());
        args.add("--job-json");
        args.add(jobJson);

        return new SparkJobSpec(job.get("jobName").toString(), mode, staging, jobJson, args);
    }

    private String stagingRoot() {
        return props.getSpark().getStagingRoot();
    }

    private String pkColumn() {
        return props.getTidb() == null || props.getTidb().getPkColumn() == null ? "id" : props.getTidb().getPkColumn();
    }

    private String readEntry() {
        return props.getApi() == null ? "inprocess" : props.getApi().getBaseUrl();
    }

    private Map<String, Object> resourceProfile() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("executorCores", props.getSpark().getExecutorCores());
        m.put("executorMemory", props.getSpark().getExecutorMemory());
        m.put("queue", props.getSpark().getQueue());
        return m;
    }

    /** TSO → 时间字符串(TSO 物理部分 = 毫秒),用于 AS OF TIMESTAMP 字面量 */
    private String toDatetime(long tso) {
        long ms = tso >> 18;
        return java.time.LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(ms), java.time.ZoneId.systemDefault())
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
