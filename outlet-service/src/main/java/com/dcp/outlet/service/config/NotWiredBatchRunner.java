package com.dcp.outlet.service.config;

import com.dcp.outlet.control.BatchRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 批阶段**占位实现**:明确拒绝而不是假装成功。
 *
 * <p>{@code startPolicy=snapshot} 的任务必须"先批打底拿 T0"才能交接。批阶段要三个真输入
 * —— changefeed 起始 TSO、TiDB GC safe point、分区最新 TSO(见 {@code T0Selector} 三规则),
 * 这三个适配器**尚未接线**;接线前若静默放行,任务会拿着错误 T0 开跑(要么丢历史、要么重复),
 * 所以这里直接让任务落到 {@code BATCH_FAILED},由人工处理。
 *
 * <p>{@code startPolicy=latest} 的任务不受影响(它不经过批阶段)。
 */
public class NotWiredBatchRunner implements BatchRunner {

    private static final Logger LOG = LoggerFactory.getLogger("outlet-batch");

    @Override
    public void runAsync(String taskId, BatchCallback callback) {
        LOG.error("任务 {} 需要批阶段(snapshot),但批阶段未接线:"
                + "T0 三输入(changefeed start_ts / GC safe point / 分区最新 TSO)缺少生产适配器;"
                + "任务置 BATCH_FAILED,请改用 startPolicy=latest 或先接线", taskId);
        callback.done(0L, false);
    }
}
