package com.dcp.subpush;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * 底座任务 params(subscription-service SubTaskDeployer 在契约 approve 时组装快照;
 * 与 tagengine-service v2TaskParams 同源同键纪律)。
 *
 * <pre>
 * {
 *   "contractId": 7, "contractName": "订单订阅", "subscriber": "appA",
 *   "viewType": "logical" | "tag",
 *   "view": { ...data_contract.view_json 快照... },
 *   "targetType": "webhook" | "kafka",
 *   "targetUrl": "https://...",                 // webhook 目标
 *   "webhookSecret": "...",                     // approve 签发, webhook HMAC 验签
 *   "topic": "dcp.sub.appA.7",                  // kafka 推送 topic(缺省 dcp.sub.{subscriber}.{id})
 *   "kafkaBootstrap": "kafka:9092",             // kafka 推送生产者
 *   "jdbcUrl"/"user"/"password": "...",         // 元库(tag 契约 exclude 剔除查 tag_fact_view; 无 exclude 可不配)
 *   "seqFloor": 1000,                           // 预留: logical 首轮全量衔接(seq ≤ floor 丢弃; 待定)
 *   "tsFloor": 1727760000000                    // 预留: tag 首轮全量衔接(事件 ts ≤ floor 丢弃; 待定)
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PushParams {

    public long contractId;
    public String contractName;
    public String subscriber;
    /** logical | tag(与 data_contract.view_type 同域) */
    public String viewType;
    /** data_contract.view_json 快照(logical: tables[]/cols[]/groups[]/merge; tag: tags[]/op/exclude[]) */
    public JsonNode view;
    /** webhook | kafka */
    public String targetType;
    public String targetUrl;
    public String webhookSecret;
    /** kafka 推送 topic; 空白时缺省 dcp.sub.{subscriber}.{contractId} */
    public String topic;
    public String kafkaBootstrap;
    /** 元库连接(tag 契约 exclude 剔除: 查 tag_fact_view 当前标签态; 无 exclude 时可为空) */
    public String jdbcUrl;
    public String user;
    public String password;
    /** 预留(首轮全量衔接方案待定): logical 信封 seq ≤ seqFloor 的记录跳过 */
    public Long seqFloor;
    /** 预留(首轮全量衔接方案待定): tag 事件 ts(epoch millis) ≤ tsFloor 的事件跳过 */
    public Long tsFloor;
}
