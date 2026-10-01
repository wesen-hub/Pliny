package com.dcp.tagtask;

import com.dcp.tag.kie.RuleAsset;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 底座任务业务参数(平台 v2TaskParams 透传; 形状与 dcp-tagstream StreamConfig 对齐):
 *
 * <pre>
 * {
 *   "taskId": 5, "entityKey": "id", "dataset": "rt_sync_c",
 *   "datasetFields": [{"name":"id","kind":"L"}, ...],
 *   "derived":  [{"name":"amt_sum","func":"sum","field":"amount","windowSeconds":60}],
 *   "bindings": [{"kind":"rule","id":10,"name":"...","ver":2}, ...],
 *   "rules":    [{"id":10,"kind":"rule","name":"...","ver":2,"def":{...}}, ...],
 *   "tagIds":   {"路径": 123},
 *   "online":   {"enabled":true,"redisHost":"dcp-redis","redisPort":6379,"ttlSeconds":3600},
 *   "factView": {"enabled":false,"jdbcUrl":"...","user":"root","password":"...","table":"tag_fact_view"},
 *   "tagEvents":{"enabled":false,"kafkaBootstrap":"kafka:9092","topic":"dcp.tag.events"}
 * }
 * </pre>
 *
 * <p>规则资产/流资产形状与 StreamConfig.RuleRef 相同(implements {@link RuleAsset}),
 * DrlGenerator 消费同一最小形状。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TagParams {

    /** 平台数值任务 id(与 v2 底座字符串 taskId 不同, 勿混用) */
    public long taskId;
    public String entityKey = "id";
    public String dataset;
    public List<FieldDef> datasetFields = new ArrayList<>();
    /** 窗口派生(滚动窗口聚合; 内存近似实现, 语义等价 Flink 版"事件级近似") */
    public List<DerivedRef> derived = new ArrayList<>();
    /** 执行序(batch task-json 同构): 有序绑定, kind=rule(平铺组) / kind=flow(规则流组) */
    public List<BindingRef> bindings = new ArrayList<>();
    /** 资产全集(rule+flow, 含 def; 发布快照闭包) */
    public List<RuleRef> rules = new ArrayList<>();
    /** 标签路径 → tag_dict 节点 id(双写 tag_fact_view/tag_notify 用; 存 id 而非路径, 标签改名不失配) */
    public Map<String, Long> tagIds = new LinkedHashMap<>();
    public OnlineRef online = new OnlineRef();
    public FactViewRef factView = new FactViewRef();
    /** 打标事件流发射(tag 契约订阅推送 dcp-sub-push 数据源; enabled 由平台开关控制) */
    public TagEventsRef tagEvents = new TagEventsRef();

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FieldDef {
        public String name;
        public String kind = "S";   // L|D|B|S
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DerivedRef {
        public String name;              // 派生字段名(规则可引用)
        public String func = "sum";      // sum|count|avg|max|min
        public String field;             // 聚合字段(数值)
        public long windowSeconds = 60;  // 滚动窗口长度(秒)
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BindingRef {
        public String kind;      // rule | flow
        public long id;
        public String name;
        public int ver;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RuleRef implements RuleAsset {
        public long id;
        public String kind;      // rule | flow
        public String name;
        public int ver;
        public JsonNode def;

        @Override public long id() { return id; }
        @Override public String kind() { return kind; }
        @Override public String name() { return name; }
        @Override public int ver() { return ver; }
        @Override public JsonNode def() { return def; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OnlineRef {
        public boolean enabled = true;
        public String redisHost = "127.0.0.1";
        public int redisPort = 6379;
        public long ttlSeconds = 3600;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FactViewRef {
        public boolean enabled = false;
        public String jdbcUrl;
        public String user = "root";
        public String password;
        public String table = "tag_fact_view";
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TagEventsRef {
        public boolean enabled = false;
        public String kafkaBootstrap = "kafka:9092";
        /** 事件流 topic(订阅推送底座任务 sub-push 消费同一 topic) */
        public String topic = "dcp.tag.events";
    }
}
