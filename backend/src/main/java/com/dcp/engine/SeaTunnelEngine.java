package com.dcp.engine;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.datasource.DsType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * SeaTunnel Zeta 执行引擎（设计文档 §4.1/§6）
 * 平台只生成/提交 JSON 配置，不持有执行线程（K1）：
 * ① 增量两段式探测（IncrementalPlanner，平台侧）
 * ② ConfigParser 生成 SeaTunnel JSON
 * ③ submit-job → 轮询 job-info(10s) → FINISHED 成功 / FAILED 失败 / 超时 stop
 * ④ metrics 采集源/目标行数
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "dcp.engine.type", havingValue = "seatunnel")
public class SeaTunnelEngine implements SyncEngine {

    private final SeaTunnelClient client;

    @Value("${dcp.engine.poll-interval-ms:10000}")
    private long pollIntervalMs;

    public SeaTunnelEngine(SeaTunnelClient client) {
        this.client = client;
    }

    @Override
    public EngineResult execute(JobRunContext ctx) throws Exception {
        String syncMode = ctx.getJob().getSyncMode();
        if ("cdc".equalsIgnoreCase(syncMode)) {
            return executeCdc(ctx);
        }
        DsType st = DsType.of(ctx.getSourceConn().get("type"));
        boolean mongoSource = st == DsType.MONGODB;
        String baseSql = mongoSource ? null : SeaTunnelConfigParser.buildExtractSql(ctx.getJob(), ctx.getExtract(), st);

        // ① 增量两段式探测（平台侧, §4.2）; Mongo 源不支持 SQL 位点, 按全量同步
        IncrementalPlanner.PlanResult plan = null;
        if (!mongoSource) {
            plan = IncrementalPlanner.plan(ctx, st, baseSql);
            if (plan.isNoData()) {
                return new EngineResult(0, 0, null, true);
            }
        }

        // ② 生成 SeaTunnel 配置（窗口 SQL 已注入 source.query）
        // SeaTunnel jobId 必须为数字; 用 executionId*100+attempt 保证重试不冲突
        int attempt = Math.max(1, ctx.getAttempt());
        String seatunnelJobId = String.valueOf(ctx.getExecutionId() * 100L + attempt);
        Map<String, String> mongoSchema = null;
        if (mongoSource) {
            List<String> fields = ctx.getMappings().stream()
                    .map(com.dcp.module.job.entity.FieldMapping::getSourceField).toList();
            mongoSchema = MongoSchemaUtil.inferFields(
                    SeaTunnelConfigParser.mongoUri(ctx.getSourceConn()),
                    SeaTunnelConfigParser.mongoDatabase(ctx.getSourceConn().get("url")),
                    ctx.getExtract().getContent().trim(), fields);
        }
        Map<String, Object> config = SeaTunnelConfigParser.build(
                ctx.getJob(), ctx.getExtract(), ctx.getMappings(),
                ctx.getSourceConn(), ctx.getTargetConn(), ctx.getSyncState(),
                ctx.getDictMaps(), seatunnelJobId, mongoSchema);
        // 注入窗口过滤 SQL（增量探测产物; 全量模式即基础 SQL; Mongo 源跳过）
        if (plan != null && plan.getSelectSql() != null) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> sources = (List<Map<String, Object>>) config.get("source");
            if (!sources.isEmpty()) {
                sources.get(0).put("query", plan.getSelectSql());
            }
        }

        // ③ 提交 + 轮询
        ctx.getInfoLog().accept("提交 SeaTunnel 任务: " + seatunnelJobId);
        String remoteId = client.submitJob(seatunnelJobId, config);
        ctx.getInfoLog().accept("SeaTunnel 引擎 jobId: " + remoteId);

        long deadline = System.currentTimeMillis() + ctx.getTimeoutMillis();
        SeaTunnelClient.JobInfo last = null;
        while (System.currentTimeMillis() < deadline) {
            checkStop(ctx);
            Thread.sleep(pollIntervalMs);
            last = client.jobInfo(remoteId);
            ctx.getInfoLog().accept("SeaTunnel 状态: " + last.getJobStatus());
            if (last.isFinished()) {
                long src = last.metricLong("SourceReceivedCount");
                long tgt = last.metricLong("SinkWriteCount");
                ctx.getInfoLog().accept("SeaTunnel 执行成功: 读取 " + src + " 行, 写入 " + tgt + " 行");
                return new EngineResult(src, tgt, plan == null ? null : plan.getWindowMax(), false);
            }
            if (last.isFailed()) {
                throw new IllegalStateException("SeaTunnel 任务失败, 状态: " + last.getJobStatus());
            }
        }
        // 超时: 停止引擎任务
        client.stopJob(remoteId);
        throw new BizException(ErrorCode.ENGINE_TIMEOUT,
                "执行超时(>" + ctx.getJob().getTimeoutMinutes() + " 分钟), 已停止 SeaTunnel 任务");
    }

    private void checkStop(JobRunContext ctx) throws InterruptedException {
        if (ctx.getStopFlag() != null && ctx.getStopFlag().get()) {
            // 停止由 ExecutionRunner 处理: 这里抛中断让外层感知
            throw new InterruptedException("任务被手动停止");
        }
    }

    /**
     * CDC 分支: MySQL-CDC 常驻任务（STREAMING）
     * - 不做增量两段式探测（binlog 位点由 SeaTunnel checkpoint 维护）
     * - 不做超时: 任务持续运行直到手动停止
     * - 停止: stopFlag 置位 -> stopJob(remoteId) -> 返回（ExecutionRunner 收尾）
     */
    private EngineResult executeCdc(JobRunContext ctx) throws Exception {
        int attempt = Math.max(1, ctx.getAttempt());
        String seatunnelJobId = String.valueOf(ctx.getExecutionId() * 100L + attempt);
        Map<String, Object> config = SeaTunnelConfigParser.build(
                ctx.getJob(), ctx.getExtract(), ctx.getMappings(),
                ctx.getSourceConn(), ctx.getTargetConn(), ctx.getSyncState(),
                ctx.getDictMaps(), seatunnelJobId, null);
        ctx.getInfoLog().accept("提交 CDC SeaTunnel 任务: " + seatunnelJobId);
        String remoteId = client.submitJob(seatunnelJobId, config);
        ctx.getInfoLog().accept("SeaTunnel 引擎 jobId: " + remoteId);

        String lastStatus = null;
        long lastMetricLog = 0L;
        while (true) {
            // 手动停止: 先停引擎侧常驻任务, 再正常返回
            if (ctx.getStopFlag() != null && ctx.getStopFlag().get()) {
                client.stopJob(remoteId);
                ctx.getInfoLog().accept("CDC 任务被手动停止, 已停止 SeaTunnel 任务 " + remoteId);
                return new EngineResult(0, 0, null, true);
            }
            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException ie) {
                // future.cancel(true) 打断轮询: 若为手动停止则正常收尾, 否则继续抛给外层
                if (ctx.getStopFlag() != null && ctx.getStopFlag().get()) {
                    client.stopJob(remoteId);
                    ctx.getInfoLog().accept("CDC 任务被手动停止(中断), 已停止 SeaTunnel 任务 " + remoteId);
                    return new EngineResult(0, 0, null, true);
                }
                Thread.currentThread().interrupt();
                throw ie;
            }
            SeaTunnelClient.JobInfo info = client.jobInfo(remoteId);
            if (info.isFailed()) {
                throw new IllegalStateException("SeaTunnel CDC 任务失败, 状态: " + info.getJobStatus());
            }
            String status = info.getJobStatus();
            if (status != null && !status.equals(lastStatus)) {
                lastStatus = status;
                ctx.getInfoLog().accept("CDC SeaTunnel 状态: " + status);
            }
            // 每 ~1 分钟记录一次 metrics（避免日志刷屏）
            long now = System.currentTimeMillis();
            if (now - lastMetricLog >= 60_000L) {
                lastMetricLog = now;
                long src = info.metricLong("SourceReceivedCount");
                long tgt = info.metricLong("SinkWriteCount");
                ctx.getInfoLog().accept("CDC 运行中: 读取 " + src + " 行, 写入 " + tgt + " 行");
            }
        }
    }
}
