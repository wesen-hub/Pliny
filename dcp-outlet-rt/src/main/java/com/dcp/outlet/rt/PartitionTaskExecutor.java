package com.dcp.outlet.rt;

import com.dcp.outlet.rt.TaskCommandCodec;
import com.dcp.scheduler.exec.PartitionExecutor;
import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.outlet.rt.CreateUnitCmd;
import com.dcp.outlet.rt.SimpleUnitMetrics;
import com.dcp.outlet.rt.UnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * outlet-task 执行器(彻底版):分区级收编 UnitRunner,框架每分区一线程一执行器。
 *
 * <pre>
 *   TaskSpec.bizMeta = TaskCommandCodec.encode(...) 整包透传(全分区起点表 + 契约/逻辑版本) ——
 *   start 时 decode 还原 CreateUnitCmd 并**按 ctx.partId 裁剪为单分区**(unitId = taskId#partId),
 *   交 UnitRunner 跑完整链路:assign → seek 注入起点 → 解码 → 水位 → T0 持续过滤
 *   → 契约裁剪(fail-closed)→ logic.process(有界重试/死信)→ commitSync。
 *   归属唯一性 / 判死 / 迁移 / 防双消费由框架统一负责(框架归属键唯一 + 保守交接双窗口),
 *   执行器无需租约、无需心跳。
 *   快照上报:processedCount=累计行数(框架据此判卡死),committedOffset=已提交游标。
 * </pre>
 */
public class PartitionTaskExecutor implements PartitionExecutor {

    private static final Logger log = LoggerFactory.getLogger(PartitionTaskExecutor.class);

    private final UnitRunner runner;
    private final SimpleUnitMetrics metrics;

    private volatile ExecutorContext ctx;
    private volatile String partitionUnitId;
    private volatile boolean stopped = true;

    public PartitionTaskExecutor(UnitRunner runner, SimpleUnitMetrics metrics) {
        this.runner = runner;
        this.metrics = metrics;
    }

    @Override
    public synchronized void start(ExecutorContext ctx) {
        this.ctx = ctx;
        stopped = false;
        // bizMeta 是控制面 → 计算面线上协议(扁平键值)整包透传;按本分区裁剪为单分区命令
        CreateUnitCmd task = TaskCommandCodec.decode(ctx.bizMeta);
        partitionUnitId = ctx.taskId + "#" + ctx.partId;
        CreateUnitCmd cmd = new CreateUnitCmd()
                .reqId(task.reqId())
                .unitId(partitionUnitId)
                .taskId(task.taskId())
                .topic(task.topic())
                .stream(task.stream())
                .contractRef(task.contractRef())
                .consumerGroup(task.consumerGroup())
                .logic(task.logic())
                .params(task.params())
                .failure(task.failure())
                .pollMillis(task.pollMillis())
                .partitions(List.of(ctx.partId))
                .startOffsets(Map.of(ctx.partId,
                        task.startOffsets().getOrDefault(ctx.partId, 0L)))
                .startTsos(Map.of(ctx.partId,
                        task.startTsos().getOrDefault(ctx.partId, 0L)));
        runner.start(cmd);
        log.info("[outlet-task] 执行器启动 ns={} task={} topic={} partition={}",
                ctx.ns, ctx.taskId, ctx.topic, ctx.partId);
    }

    @Override
    public synchronized void stop() {
        stopped = true;
        if (partitionUnitId != null) {
            runner.stopAndForget(partitionUnitId, 5000);   // 优雅停机并遗忘(迁移回本 worker 时可同 id 重启)
        }
        log.info("[outlet-task] 执行器停止 task={} partition={}",
                ctx == null ? "?" : ctx.taskId, ctx == null ? -1 : ctx.partId);
    }

    @Override
    public ExecutorSnapshot metrics() {
        if (partitionUnitId == null) {
            return ExecutorSnapshot.of(ctx == null ? "?" : ctx.taskId, ctx == null ? -1 : ctx.partId,
                    0, 0, 0, 0, stopped);
        }
        long committed = runner.lastCommittedOffset(partitionUnitId);
        // 执行器 FAILED(逻辑加载/解码异常退出)→ 快照置 stopped,让框架判死重分配(框架统一兜底,
        // 不留"归属仍在但消费已停"的死分区;防双消费仍由归属键唯一保证)
        boolean down = stopped || runner.state(partitionUnitId) == UnitRunner.State.FAILED;
        // processedCount = 累计处理行数(框架活性检测用;committedOffset = 分区已提交游标)
        return ExecutorSnapshot.of(ctx.taskId, ctx.partId, metrics.rows(),
                committed < 0 ? 0 : committed,
                metrics.count("failure") + metrics.count("dlq"),
                0, down);
    }
}
