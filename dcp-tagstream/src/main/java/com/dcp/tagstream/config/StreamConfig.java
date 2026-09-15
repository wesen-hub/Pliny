package com.dcp.tagstream.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * 流执行作业配置(平台生成 job-json, 结构对齐 tagengine task-json 快照 + 流运行参数):
 *
 * <pre>
 * {
 *   "taskId": 5, "taskVer": 2, "entityKey": "id",
 *   "datasetFields": ["id","status","region"],
 *   "kafka": {"bootstrapServers":"kafka:9092","topic":"dcp.in.customer_wide","groupId":"dcp-tagstream-t5"},
 *   "rules": [/* 发布快照 assets(含 def) *\/],
 *   "online": {"redisHost":"dcp-redis","redisPort":6379,"ttlSeconds":3600,"enabled":true},
 *   "factView": {"jdbcUrl":"jdbc:mysql://.../dcp?...","user":"root","password":"...","table":"tag_fact_view","enabled":true}
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamConfig implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    public long taskId;
    public int taskVer = 1;
    public String entityKey = "id";
    public List<FieldDef> datasetFields = new ArrayList<>();

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FieldDef implements java.io.Serializable { private static final long serialVersionUID = 1L;
        public String name;
        public String kind = "S";   // L|D|B|S
    }
    public KafkaRef kafka = new KafkaRef();
    /** 执行序(batch task-json 同构): 有序绑定, kind=rule(平铺组) / kind=flow(规则流组) */
    public List<BindingRef> bindings = new ArrayList<>();
    /** 资产全集(rule+flow, 含 def; 发布快照闭包) —— 命名沿用 rules 以兼容旧 job-json(空 bindings 时全按 rule 平铺) */
    public List<RuleRef> rules = new ArrayList<>();
    /** 窗口派生(滚动窗口聚合; 事件到达用当前窗口累计值参与求值, 窗口边界重置) */
    public List<DerivedRef> derived = new ArrayList<>();
    public OnlineRef online = new OnlineRef();
    public FactViewRef factView = new FactViewRef();

    /**
     * 标签路径 → tag_dict 节点 id(平台下发):用于双写 `tag_fact_view` / `tag_notify` 的 `tag_dict_id`。
     * <p>存 id 而非路径 —— 标签改名/移动时规则与结果不再失配。
     */
    public java.util.Map<String, Long> tagIds = new java.util.LinkedHashMap<>();

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BindingRef implements java.io.Serializable { private static final long serialVersionUID = 1L;
        public String kind;      // rule | flow
        public long id;
        public String name;
        public int ver;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DerivedRef implements java.io.Serializable { private static final long serialVersionUID = 1L;
        public String name;              // 派生字段名(规则可引用)
        public String func;              // sum|count|avg|max|min
        public String field;             // 聚合字段(数值)
        public long windowSeconds = 60;  // 滚动窗口长度(秒)
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class KafkaRef implements java.io.Serializable { private static final long serialVersionUID = 1L;
        public String bootstrapServers = "kafka:9092";
        public String topic;
        public String groupId;
        /** 每分区起始 offset(方案 C:v2 播种的起点;null/空 = 从 latest 起) */
        public java.util.Map<Integer, Long> partitionStartOffsets;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OnlineRef implements java.io.Serializable { private static final long serialVersionUID = 1L;
        public boolean enabled = true;
        public String redisHost = "dcp-redis";
        public int redisPort = 6379;
        public long ttlSeconds = 3600;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FactViewRef implements java.io.Serializable { private static final long serialVersionUID = 1L;
        public boolean enabled = false;
        public String jdbcUrl;
        public String user = "root";
        public String password;
        public String table = "tag_fact_view";
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RuleRef implements java.io.Serializable { private static final long serialVersionUID = 1L;
        public long id;
        public String kind;      // rule | flow
        public String name;
        public int ver;
        public JsonNode def;
    }
}
