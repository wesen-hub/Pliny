package com.dcp.module.outlet.source;

import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.StreamStatus;
import com.dcp.module.outlet.model.SnapshotPage;

import java.util.Map;

/**
 * 变更流数据源 SPI(设计 §6.1:出口对上游只依赖这一层)。
 *
 * <p>替换点:
 * <ul>
 *   <li>骨架期:{@link MockChangeStreamSource}(内存事件,可跑通全部接口与权限/位点语义);</li>
 *   <li>落地期:{@code KafkaChangeStreamSource}(消费 L1 内部流 dcp.cdc.{schema}.{table},
 *       canal-json + WATERMARK;按契约投影后产出同一批事件模型)—— 接口签名不变。</li>
 * </ul>
 *
 * <p>所有方法里的 TSO 均为 PD 全局单调时间戳(路径 3 的位点语义)。
 */
public interface ChangeStreamSource {

    /** 接口 ① stream-status:{epoch, earliest, latest, watermarkTs, retentionHours} */
    StreamStatus status(String streamId);

    /** 当前 watermark(所有 commitTs < watermarkTs 的事件均已发出);无水位返回 null */
    Long watermark(String streamId);

    /**
     * 接口 ③ changes:增量拉取。
     *
     * @param afterTso 从哪个位点之后开始(不含)
     * @param epoch    调用方位点的代次,必须与流当前代次一致
     * @param limit    本批最多事件数
     */
    ChangeBatch changes(String streamId, long afterTso, int epoch, int limit);

    /**
     * 接口 ② snapshot:快照分页(同一 boundary 的页序列必须稳定)。
     *
     * @param pageToken null 表示第一页
     */
    SnapshotPage snapshot(String streamId, long boundaryTso, int epoch, String pageToken, int limit);

    /** 接口 ⑤ position-at:时间戳 → 位点 */
    long positionAt(String streamId, long timestampMs);

    /** 接口 ④ key?asOf=:按主键回查(仅兜底对账;常规消费**不回表**) */
    Map<String, Object> keyLookup(String streamId, Map<String, Object> bizKey, Long asOfTso);

    /**
     * 流当前可见的列(用于计算契约裁掉了哪些列,审计要能解释"为什么少了几列")。
     * 返回 null 表示"数据源不提供列信息"(骨架场景),此时 trimmedColumns 记为空。
     */
    default java.util.Set<String> availableColumns(String streamId) {
        return null;
    }

    /**
     * 估算快照行数(供 v2 批阶段切片/执行体选择用)。
     * <p>未知 / 失败一律返回 -1(调用方据此退化,不猜);实现=真实数据源 COUNT(*) AS OF T0。
     */
    default long estimateRows(String streamId, long boundaryTso) {
        return -1L;
    }
}
