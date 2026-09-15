package com.dcp.outlet.control;

import java.util.List;
import java.util.Optional;

/**
 * 流登记表(新表感知的产物)。
 *
 * <p>新建表 → TiCDC 自动建该表 topic → 索引器感知 → **登记为"可消费的流"**;
 * 建任务时按逻辑表名查这里拿 topic 与分区数 —— **逻辑表名 → 物理 topic 的唯一入口**。
 */
public interface StreamRegistry {

    Optional<StreamInfo> get(String stream);

    List<StreamInfo> list();

    /** 登记(新表感知扫描时调用);已存在则更新。 */
    void register(StreamInfo info);
}
