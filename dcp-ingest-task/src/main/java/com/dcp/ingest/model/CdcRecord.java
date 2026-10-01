package com.dcp.ingest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 统一消息模型(与 com.dcp.sync.CdcRecord JSON 结构一致, 字段名逐一对齐;
 * 独立复制进 fat jar 自足, 避免共享依赖 —— 与 dcp-synclink model 包同纪律)。
 *
 * <p>入站消费(KafkaIngestEngine/dcp-ingest-task)打信封专用:
 * taskId=k{appKey}, sourceDb=kafka, sourceTable=ingest, datasourceId=k。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CdcRecord {

    /** 逻辑表名 */
    public String logicalTable;
    /** 任务实例 id(seq 写者列维度: 每任务×源一条序列) */
    public String taskId;
    /** 数据源 id / 源库 / 源表(流身份, 审计与归属用) */
    public String datasourceId;
    public String sourceDb;
    public String sourceTable;
    /** 业务主键(已归一化, 如 "ERP|100231") */
    public String businessKey;
    /** 主键修改场景: 变更前业务主键; 入站无主键修改语义, 恒 null(不序列化) */
    public String beforeKey;
    /** 目标唯一键列(幂等定位用) */
    public List<String> keyColumns;
    /** INSERT | UPDATE | DELETE */
    public String rowKind;
    /** 全局 seq(记录级, 目标侧裁判新旧) */
    public long seq;
    /** 快照标志(入站恒 null, 默认走流模板) */
    public Boolean snapshot;
    /** 业务字段(值均已归一化为字符串) */
    public Map<String, Object> fields = new LinkedHashMap<>();

    public static CdcRecord of(String logicalTable, String businessKey,
                               List<String> keyColumns, String rowKind, long seq,
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
