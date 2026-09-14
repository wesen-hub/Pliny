package com.dcp.module.outlet.model;

import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 规范化后的变更事件(设计 §6.3 / §5.5)。
 *
 * <p>关键语义:
 * <ul>
 *   <li>{@code row} = **变更后整行**(来源 canal-json 的 data)——消费方按主键 upsert,**不回表**;</li>
 *   <li>{@code before} = 变更前值(仅 UPDATE 有,来源 old)——裁剪/墓碑/主键变更识别靠它;</li>
 *   <li>{@code tso} = {@code _tidb.commitTs},**事件自带位点**,无需额外查询;</li>
 *   <li>{@code outOfScope} = 条件退出(行仍存在但不再满足契约条件)→ 下发给消费方时按删除处理;</li>
 *   <li>内部列({@code _pk_id}/{@code _seq_*}/密级超限列)在出口投影阶段**直接剔除**,连 key 都不出现。</li>
 * </ul>
 */
@Data
public class ChangeEvent {

    /** INSERT | UPDATE | DELETE */
    private String op;
    /** 逻辑表(streamId),如 t_order */
    private String streamId;
    /** 业务主键 */
    private Map<String, Object> key = new LinkedHashMap<>();
    /** 变更后整行(裁列后) */
    private Map<String, Object> row = new LinkedHashMap<>();
    /** 变更前值(裁列后;仅 UPDATE) */
    private Map<String, Object> before;
    /** 事件时间(源端 ts,毫秒) */
    private Long ts;
    /** 事务提交 TSO */
    private long tso;
    /** 流代次 */
    private int epoch;
    /** 条件退出标记 */
    private boolean outOfScope;
}
