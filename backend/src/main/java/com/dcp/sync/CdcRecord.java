package com.dcp.sync;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 统一消息模型: 采集端打标后写入 Kafka 的记录(V3 §3.1/§4.4)。
 *
 * <ul>
 *   <li>businessKey: 业务主键(采集端归一后, 纯函数路由的输入, 与物理位置无关)</li>
 *   <li>keyColumns:  目标表唯一键列(用于幂等写定位行; P0 由采集端携带)</li>
 *   <li>rowKind:     操作类型 INSERT | UPDATE | DELETE</li>
 *   <li>seq:         全局流水号(入口打标, 记录级)</li>
 *   <li>fields:      业务字段(值均已归一化)</li>
 * </ul>
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CdcRecord {

    /** 逻辑表名 */
    private String logicalTable;
    /** 任务实例 id(seq 写者列维度: 每任务×源一条序列) */
    private String taskId;
    /** 数据源 id / 源库 / 源表(流身份, 审计与归属用) */
    private String datasourceId;
    private String sourceDb;
    private String sourceTable;
    /** 业务主键(已归一化, 如 "ERP|100231" 或 "100231") */
    private String businessKey;
    /** 主键修改场景: 变更前的业务主键(before PK ≠ businessKey → 采集端已停任务告警, 消费端兑底落死信); 非主键修改为 null */
    private String beforeKey;
    /** 目标唯一键列(幂等定位用) */
    private java.util.List<String> keyColumns;
    /** INSERT | UPDATE | DELETE */
    private String rowKind;
    /** 全局 seq(记录级, 目标侧裁判新旧) */
    private long seq;
    /** 快照标志(true=批阶段快照行, 写入端据此选批模板; 补丁落地前为 null → 默认走流模板) */
    private Boolean snapshot;
    /** 业务字段 */
    private Map<String, Object> fields = new LinkedHashMap<>();

    public static CdcRecord of(String logicalTable, String businessKey,
                               java.util.List<String> keyColumns, String rowKind, long seq,
                               Map<String, Object> fields) {
        CdcRecord r = new CdcRecord();
        r.logicalTable = logicalTable;
        r.businessKey = businessKey;
        r.keyColumns = keyColumns;
        r.rowKind = rowKind;
        r.seq = seq;
        if (fields != null) r.fields = fields;
        return r;
    }
}
