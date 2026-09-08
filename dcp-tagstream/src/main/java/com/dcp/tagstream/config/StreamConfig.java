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
    public List<String> datasetFields = new ArrayList<>();
    public KafkaRef kafka = new KafkaRef();
    public List<RuleRef> rules = new ArrayList<>();
    public OnlineRef online = new OnlineRef();
    public FactViewRef factView = new FactViewRef();

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class KafkaRef implements java.io.Serializable { private static final long serialVersionUID = 1L;
        public String bootstrapServers = "kafka:9092";
        public String topic;
        public String groupId;
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
