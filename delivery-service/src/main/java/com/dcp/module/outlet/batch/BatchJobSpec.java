package com.dcp.module.outlet.batch;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 批作业声明(设计 §6.3:消费方**不碰 Spark 作业生命周期**,只声明输入契约 + 转换逻辑 + 目标)。
 */
@Data
public class BatchJobSpec {

    private String consumerId;
    private String streamId;
    /** bootstrap(全量建仓) | incr_batch(区间批次) | mixed(bootstrap 后接增量) */
    private String jobType = "bootstrap";
    /** 目标表 / 输出路径 */
    private String target;
    /** bootstrap 的边界 T0(为空则由平台取当前 latest) */
    private Long boundaryTso;
    /** 增量批区间右端(为空则取 watermark) */
    private Long hiTso;
    /** 转换逻辑标识(SQL 文件名 / UDF 类名;由 Spark 作业侧解释) */
    private String transform;
    /** 一次拉取的事件数上限 */
    private int batchLimit = 500;
    /** 快照页大小 */
    private int snapshotLimit = 1000;

    public List<String> validate() {
        List<String> errs = new ArrayList<>();
        if (consumerId == null || consumerId.isBlank()) {
            errs.add("consumerId 必填");
        }
        if (streamId == null || streamId.isBlank()) {
            errs.add("streamId 必填");
        }
        if (target == null || target.isBlank()) {
            errs.add("target 必填(目标表/输出路径)");
        }
        if (!List.of("bootstrap", "incr_batch", "mixed").contains(jobType)) {
            errs.add("jobType 非法:" + jobType);
        }
        return errs;
    }
}
