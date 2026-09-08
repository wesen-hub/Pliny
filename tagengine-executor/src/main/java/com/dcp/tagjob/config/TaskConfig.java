package com.dcp.tagjob.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * 任务执行配置(task-json, 平台侧落盘)——字段与后端对齐(设计文档 2026-09-08 §12; 后端 TagEngineService.tag_task)。
 *
 * <pre>
 * {
 *   "taskId":1, "taskVer":2, "dataset":"customer_wide", "entityKey":"customer_id",
 *   "iceberg":{"catalog":"demo","namespace":"dcp","table":"customer_wide"},
 *   "datasetFields":[{"name":"customer_id","type":"long"},{"name":"status","type":"string"}],
 *   "derived":[{"name":"pay7d","func":"SUM","field":"amount","windowDays":7}],
 *   "bindings":[{"kind":"rule","id":1,"ver":2,"name":"金额档"},{"kind":"flow","id":3,"ver":1,"name":"风控链"}],
 *   "rules":[/* 全部被引用资产定义快照, 与后端 tag_rule.def_json 一致 *\/],
 *   "tagFact":{"catalog":"demo","namespace":"dcp","table":"tag_fact","partitionByTask":true}
 * }
 * </pre>
 * 语义: 执行序 = bindings 数组序, 但规则(rule)先跑 → 规则流(flow)后跑(§3b/§11①)。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskConfig {
    public long taskId;
    public int taskVer = 1;
    /** 数据集 = 逻辑表名(湖落地由外部推进; 联调可用 spark.read 抽象兜底) */
    public String dataset;
    /** 实体键字段(行级求值/事实层 entity 列取值) */
    public String entityKey;
    public IcebergRef iceberg = new IcebergRef();
    public List<FieldDef> datasetFields = new ArrayList<>();
    public List<DerivedDef> derived = new ArrayList<>();
    /** 绑定快照 [{kind,id,ver,name}] 数组序; 语义=规则先跑→流后跑 */
    public List<BindingRef> bindings = new ArrayList<>();
    /** 全部被引用资产定义快照(与后端 tag_rule.def_json 一致), id 为键 */
    public List<RuleSnapshot> rules = new ArrayList<>();
    public TagFactRef tagFact = new TagFactRef();

    /** Iceberg 湖表引用(数据集/事实层共用结构) */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class IcebergRef {
        public String catalog;
        public String namespace;
        public String table;
        /** 可选: hadoop catalog 仓库路径(spark.sql.catalog.{catalog}.warehouse 装配用; dev E2E) */
        public String warehouse;
        /** 可选: 驱动类缺省 org.apache.iceberg.spark.SparkCatalog */
        public String catalogType = "hadoop";

        public String qualified() {
            StringBuilder sb = new StringBuilder();
            if (notBlank(catalog)) sb.append(catalog).append('.');
            if (notBlank(namespace)) sb.append(namespace).append('.');
            sb.append(table);
            return sb.toString();
        }

        public boolean isBlank() {
            return !notBlank(table);
        }
    }

    /** 数据集字段清单声明(发布依赖域用; 执行器与真实 schema 做交集校验) */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FieldDef {
        public String name;
        public String type;
    }

    /** 派生字段: (a) expr 纯表达式 (b) func+field 聚合(实体级 groupBy) */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DerivedDef {
        public String name;
        /** (a) 纯表达式: Spark SQL 函数子集, 用 spark.sql 函数 expr() */
        public String expr;
        /** (b) 聚合派生: SUM/COUNT/AVG/MIN/MAX */
        public String func;
        public String field;
        /** 时间窗(天); 本期简化不做跨天窗口回看, 见 TagBatchJob.assembleDerived 注释 */
        public int windowDays;
        /** 事件时间列名(配合 windowDays 过滤) */
        public String windowColumn;

        public boolean isAgg() {
            return notBlank(func) && notBlank(field);
        }

        public boolean isExpr() {
            return notBlank(expr);
        }
    }

    /** 绑定引用(kind=rule|flow) */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BindingRef {
        public String kind;
        public long id;
        public int ver;
        public String name;
    }

    /** 资产定义快照(与后端 tag_rule.def_json 对齐): kind=rule 的 def{visual|table|drl}; kind=flow 的 steps[] */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RuleSnapshot {
        public long id;
        public String kind;
        public String name;
        public int ver;
        public JsonNode def;
    }

    /** tag_fact 事实层写目标 */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TagFactRef {
        public String catalog;
        public String namespace;
        public String table = "tag_fact";
        /** true: 按 task_id 分区, 每轮 当前值=先删本任务旧分区再 append(留史靠 time travel) */
        public boolean partitionByTask = true;

        public String qualified() {
            StringBuilder sb = new StringBuilder();
            if (notBlank(catalog)) sb.append(catalog).append('.');
            if (notBlank(namespace)) sb.append(namespace).append('.');
            sb.append(table);
            return sb.toString();
        }
    }

    /** 增量配置(§14): 缺省 = 全量模式。增量 = 只读 _seq>cursor 的变更行(墓碑行=清标签不重算), 只对变更实体覆盖写。 */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Incremental {
        /** 增量游标: 上轮已处理的湖表行级 seq 最大值; <0 或缺省 = 全量首轮 */
        public long cursor = -1;
        /** 增量列(行级变更序): 缺省自动探测湖表 _seq% 列; 多列取 max(需同源推进语义) */
        public String seqColumn;
        /** 墓碑列: 缺省 _is_deleted; 该列为 true 的行 = 实体注销, 只删 tag_fact 旧行不重算 */
        public String tombstoneColumn = "_is_deleted";

        public boolean enabled() {
            return cursor >= 0;
        }
    }

    public Incremental incremental = new Incremental();

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }
}
