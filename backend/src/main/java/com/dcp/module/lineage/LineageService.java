package com.dcp.module.lineage;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 血缘解析与查询(2026-09-09 方案 §4/§6, P0 同步层):
 * 图模型(Neo4j):
 *   (:Table {id:'table:{name}', name, layer?, domain?, kind:'model|logical|physical'})  逻辑表/模型表/物理落库表(核心节点)
 *   (:Field {id:'field:{table}.{col}', name, table})  -[:CONTAINS]-(Table) 字段级
 *   (:Source {id:'ds:{id}', name, dbType}) 源系统
 *   (:Job {id:'job:{id}', name, type:'sync'})  (Source)-[:SOURCE]->(Job)-[:SOURCE_OF]->(Table)
 *        (Job)-[:FIELD_MAPPING {sourceField,targetField,transform}]->(Field)  字段级映射边(多源合流=同 Field 多条)
 *   (:App {id:'contract:{id}', name, kind:'SUBSCRIBE', subscriber}) -[:CONSUMES]->(Table)  订阅下游
 * 解析源(全部真实存量): model_definition / logical_table / job_definition+field_mapping / datasource_config / data_contract(view_json)
 * P0 边界: 同步层 + 订阅消费表级; 规则/打标层(P1), 字段级消费列(P1); 源表名缺失(平台未登记)时不虚构源表, 载荷带 sourceField。
 */
@Service
public class LineageService {

    private static final Logger log = LoggerFactory.getLogger(LineageService.class);
    private static final ObjectMapper OM = new ObjectMapper();

    private final JdbcTemplate meta;
    private final Driver driver;
    private final String targetUrl;
    private final String targetUser;
    private final String targetPassword;
    private volatile JdbcTemplate target;

    public LineageService(JdbcTemplate meta, Driver driver,
                          @Value("${dcp.sync.target.url}") String targetUrl,
                          @Value("${dcp.sync.target.username:root}") String username,
                          @Value("${dcp.sync.target.password:root123}") String password) {
        this.meta = meta;
        this.driver = driver;
        this.targetUrl = targetUrl;
        this.targetUser = username;
        this.targetPassword = password;
    }

    private static String s(Object o) { return o == null ? "" : String.valueOf(o); }

    private JdbcTemplate targetJdbc() {
        if (target == null) {
            synchronized (this) {
                if (target == null) {
                    DriverManagerDataSource ds = new DriverManagerDataSource();
                    ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
                    ds.setUrl(targetUrl);
                    ds.setUsername(targetUser);
                    ds.setPassword(targetPassword);
                    target = new JdbcTemplate(ds);
                }
            }
        }
        return target;
    }

    private Session session() {
        if (driver == null) throw new BizException(ErrorCode.PARAM_ERROR, "血缘图库(Neo4j)不可用");
        return driver.session();
    }

    /* ================= 全量重建(幂等: 清图后按真实存量重建) ================= */

    /** 启动就绪后自动全量重建(幂等; Neo4j 未就绪时跳过, 可手动 POST /api/lineage/rebuild) */
    @org.springframework.context.event.EventListener(org.springframework.boot.context.event.ApplicationReadyEvent.class)
    public void onReady() {
        try {
            if (driver != null) rebuild();
        } catch (Exception e) {
            log.warn("启动血缘图重建跳过: {}", e.getMessage());
        }
    }

    public Map<String, Object> rebuild() {
        long t0 = System.currentTimeMillis();
        List<Map<String, Object>> tables = collectTables();   // id/name/layer/domain/kind + 字段列
        List<Map<String, Object>> jobs = meta.queryForList(
                "SELECT j.id, j.job_name, j.target_table_name, j.sync_mode, j.source_datasource_id, d.name ds_name, d.type db_type"
                        + " FROM job_definition j LEFT JOIN datasource_config d ON d.id=j.source_datasource_id WHERE j.deleted=0 OR j.deleted IS NULL");
        List<Map<String, Object>> mappings = meta.queryForList(
                "SELECT job_id, source_field, target_field, transform_type FROM field_mapping");
        List<Map<String, Object>> contracts = meta.queryForList(
                "SELECT id, name, subscriber, view_json FROM data_contract WHERE status IN ('active','pending')");
        Set<String> tableIds = new HashSet<>();
        tables.forEach(t -> tableIds.add("table:" + t.get("name")));

        try (Session se = session()) {
            // 约束须在独立事务(schema 修改不能跟在 write 之后)
            se.executeWrite(tx -> {
                tx.run("CREATE CONSTRAINT table_id IF NOT EXISTS FOR (n:Table) REQUIRE n.id IS UNIQUE");
                tx.run("CREATE CONSTRAINT field_id IF NOT EXISTS FOR (n:Field) REQUIRE n.id IS UNIQUE");
                tx.run("CREATE CONSTRAINT job_id IF NOT EXISTS FOR (n:Job) REQUIRE n.id IS UNIQUE");
                tx.run("CREATE CONSTRAINT app_id IF NOT EXISTS FOR (n:App) REQUIRE n.id IS UNIQUE");
                tx.run("CREATE CONSTRAINT src_id IF NOT EXISTS FOR (n:Source) REQUIRE n.id IS UNIQUE");
                return null;
            });
            se.executeWrite(tx -> {
                tx.run("MATCH (n) DETACH DELETE n");
                // 表 + 字段
                int fields = 0;
                for (Map<String, Object> t : tables) {
                    tx.run("MERGE (a:Table {id:$id}) SET a.name=$name, a.layer=$layer, a.domain=$domain, a.kind=$kind",
                            Map.of("id", "table:" + t.get("name"), "name", s(t.get("name")), "layer", s(t.get("layer")),
                                    "domain", s(t.get("domain")), "kind", s(t.get("kind"))));
                    Object cols = t.get("cols");
                    if (cols instanceof List<?> colList) {
                        for (Object c : colList) {
                            String col = s(c);
                            if (col.isBlank()) continue;
                            tx.run("MERGE (a:Table {id:$tid}) MERGE (f:Field {id:$fid}) SET f.name=$col, f.table=$tb MERGE (a)-[:CONTAINS]->(f)",
                                    Map.of("tid", "table:" + t.get("name"), "fid", "field:" + t.get("name") + "." + col,
                                            "col", col, "tb", s(t.get("name"))));
                            fields++;
                        }
                    }
                }
                // Job + 源系统
                for (Map<String, Object> j : jobs) {
                    String jid = "job:" + j.get("id");
                    tx.run("MERGE (j:Job {id:$id}) SET j.name=$n, j.type='sync', j.syncMode=$sm",
                            Map.of("id", jid, "n", s(j.get("job_name")), "sm", s(j.get("sync_mode"))));
                    if (j.get("source_datasource_id") != null) {
                        String dsId = "ds:" + j.get("source_datasource_id");
                        tx.run("MERGE (d:Source {id:$id}) SET d.name=$n, d.dbType=$t", Map.of("id", dsId, "n", s(j.get("ds_name")), "t", s(j.get("db_type"))));
                        tx.run("MATCH (d:Source {id:$did}), (j:Job {id:$jid}) MERGE (d)-[:SOURCE]->(j)", Map.of("did", dsId, "jid", jid));
                    }
                    String tt = s(j.get("target_table_name"));
                    if (!tt.isBlank() && tableIds.contains("table:" + tt)) {
                        tx.run("MATCH (j:Job {id:$jid}), (a:Table {id:$tid}) MERGE (j)-[:SOURCE_OF]->(a)", Map.of("jid", jid, "tid", "table:" + tt));
                    }
                }
                // 字段级映射(多源合流=同一 Field 多条 FIELD_MAPPING 入边)
                for (Map<String, Object> m : mappings) {
                    String jid = "job:" + m.get("job_id");
                    String tf = s(m.get("target_field"));
                    if (tf.isBlank()) continue;
                    String tt = null;
                    for (Map<String, Object> j : jobs) {
                        if (s(j.get("id")).equals(s(m.get("job_id")))) { tt = s(j.get("target_table_name")); break; }
                    }
                    if (tt == null || tt.isBlank()) continue;
                    String fid = "field:" + tt + "." + tf;
                    tx.run("MATCH (j:Job {id:$jid}), (f:Field {id:$fid}) MERGE (j)-[r:FIELD_MAPPING]->(f) SET r.sourceField=$sf, r.targetField=$tf, r.transform=$tr",
                            Map.of("jid", jid, "fid", fid, "sf", s(m.get("source_field")), "tf", tf, "tr", s(m.get("transform_type"))));
                }
                // 订阅消费(表级; 字段级引用列 P1)
                for (Map<String, Object> c : contracts) {
                    String aid = "contract:" + c.get("id");
                    tx.run("MERGE (a:App {id:$id}) SET a.name=$n, a.kind='SUBSCRIBE', a.subscriber=$sub",
                            Map.of("id", aid, "n", s(c.get("name")), "sub", s(c.get("subscriber"))));
                    try {
                        JsonNode view = OM.readTree((String) c.get("view_json"));
                        view.path("tables").forEach(tn -> {
                            String tb = tn.asText();
                            if (tableIds.contains("table:" + tb)) {
                                tx.run("MATCH (a:App {id:$aid}), (t:Table {id:$tid}) MERGE (a)-[:CONSUMES]->(t)",
                                        Map.of("aid", aid, "tid", "table:" + tb));
                            }
                        });
                    } catch (Exception ignore) { }
                }
                tx.run("MATCH (f:Field) WHERE NOT (f)<-[:CONTAINS]-() WITH f LIMIT 200 DETACH DELETE f");
                return null;
            });
        } catch (Exception e) {
            log.warn("血缘图重建失败: {}", e.getMessage());
            throw new BizException(ErrorCode.PARAM_ERROR, "血缘图重建失败: " + e.getMessage());
        }
        log.info("血缘图重建完成: 表 {} / 字段映射 {} / 任务 {} / 契约 {} / {}ms", tables.size(), mappings.size(), jobs.size(), contracts.size(), System.currentTimeMillis() - t0);
        return Map.of("tables", tables.size(), "mappings", mappings.size(), "jobs", jobs.size(), "contracts", contracts.size(), "ms", System.currentTimeMillis() - t0);
    }

    /** 表资产: target 库全表 + 模型层/域标注 + 逻辑表; 字段=information_schema 探测 */
    private List<Map<String, Object>> collectTables() {
        Map<String, String> modelLayer = new HashMap<>(), modelDomain = new HashMap<>();
        meta.queryForList("SELECT table_name, layer, domain FROM model_definition").forEach(r -> {
            modelLayer.put(s(r.get("table_name")), s(r.get("layer")));
            modelDomain.put(s(r.get("table_name")), s(r.get("domain")));
        });
        Set<String> logical = new HashSet<>();
        meta.queryForList("SELECT table_name FROM logical_table").forEach(r -> logical.add(s(r.get("table_name"))));
        Set<String> jobTables = new HashSet<>();
        meta.queryForList("SELECT DISTINCT target_table_name FROM job_definition WHERE target_table_name IS NOT NULL AND target_table_name<>''")
                .forEach(r -> jobTables.add(s(r.get("target_table_name"))));
        List<Map<String, Object>> out = new ArrayList<>();
        try {
            List<Map<String, Object>> trows = targetJdbc().queryForList(
                    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME NOT LIKE '%authority%' AND TABLE_NAME NOT IN ('wqe') ORDER BY TABLE_NAME");
            for (Map<String, Object> t : trows) {
                String name = s(t.get("TABLE_NAME"));
                String layer = modelLayer.getOrDefault(name, logical.contains(name) ? "ODS" : (jobTables.contains(name) ? "ODS" : ""));
                String domain = modelDomain.getOrDefault(name, "");
                String kind = modelLayer.containsKey(name) ? "model" : (logical.contains(name) ? "logical" : "physical");
                List<String> cols = new ArrayList<>();
                targetJdbc().queryForList("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?"
                                + " AND COLUMN_NAME NOT IN ('_is_deleted') ORDER BY ORDINAL_POSITION", name)
                        .forEach(c -> cols.add(s(c.get("COLUMN_NAME"))));
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("name", name); m.put("layer", layer); m.put("domain", domain); m.put("kind", kind); m.put("cols", cols);
                out.add(m);
            }
        } catch (Exception e) {
            log.warn("目标库扫描失败(仅用登记表): {}", e.getMessage());
            Set<String> names = new LinkedHashSet<>();
            names.addAll(modelLayer.keySet()); names.addAll(logical); names.addAll(jobTables);
            for (String n : names) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("name", n); m.put("layer", modelLayer.getOrDefault(n, "ODS"));
                m.put("domain", modelDomain.getOrDefault(n, "")); m.put("kind", "physical");
                m.put("cols", List.of());
                out.add(m);
            }
        }
        return out;
    }

    /* ================= 查询(路径集, 不截断) ================= */

    /** 上游(溯源): b -[*]-> a ; 返回多条路径 */
    public List<Map<String, Object>> upstream(String table, Integer depth) {
        return paths("table:" + table, depth == null ? 4 : depth, true);
    }

    /** 下游(影响): a -[*]-> b */
    public List<Map<String, Object>> downstream(String table, Integer depth) {
        return paths("table:" + table, depth == null ? 3 : depth, false);
    }

    /** 字段链路(溯源+消费, 起点为字段节点) */
    public Map<String, Object> fieldTrace(String table, String col) {
        String fid = "field:" + table + "." + col;
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("upstream", paths(fid, 4, true));
        out.put("downstream", paths(fid, 3, false));
        return out;
    }

    private List<Map<String, Object>> paths(String id, int depth, boolean up) {
        String cypher = up
                ? "MATCH p=(b)-[r*1.." + depth + "]->(a) WHERE a.id=$id RETURN [x IN nodes(p)|{id:x.id,label:coalesce(x.name,x.subscriber,x.id),type:head(labels(x))}] AS ns, [x IN relationships(p)|{type:type(x),sourceField:x.sourceField,targetField:x.targetField,transform:x.transform}] AS rs LIMIT 200"
                : "MATCH p=(a)-[r*1.." + depth + "]->(b) WHERE a.id=$id RETURN [x IN nodes(p)|{id:x.id,label:coalesce(x.name,x.subscriber,x.id),type:head(labels(x))}] AS ns, [x IN relationships(p)|{type:type(x),sourceField:x.sourceField,targetField:x.targetField,transform:x.transform}] AS rs LIMIT 200";
        List<Map<String, Object>> out = new ArrayList<>();
        try (Session se = session()) {
            se.executeRead(tx -> {
                var res = tx.run(cypher, Map.of("id", id));
                while (res.hasNext()) {
                    Record rec = res.next();
                    List<Map<String, Object>> ns = new ArrayList<>();
                    rec.get("ns").values().forEach(v -> ns.add(v.asMap()));
                    List<Map<String, Object>> rs = new ArrayList<>();
                    rec.get("rs").values().forEach(v -> rs.add(v.asMap()));
                    Map<String, Object> p = new LinkedHashMap<>();
                    p.put("nodes", ns);
                    p.put("edges", rs);
                    out.add(p);
                }
                return null;
            });
        } catch (Exception e) {
            throw new BizException(ErrorCode.PARAM_ERROR, "血缘查询失败: " + e.getMessage());
        }
        return out;
    }

    /** 全图(前端图谱接真用) */
    public Map<String, Object> graph() {
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<Map<String, Object>> edges = new ArrayList<>();
        try (Session se = session()) {
            se.executeRead(tx -> {
                var nr = tx.run("MATCH (n) RETURN n.id AS id, coalesce(n.name,n.subscriber,n.id) AS label, head(labels(n)) AS type, n.layer AS layer, n.domain AS domain, n.kind AS kind, n.dbType AS dbType, n.subscriber AS subscriber, n.syncMode AS syncMode");
                while (nr.hasNext()) {
                    Record r = nr.next();
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", r.get("id").asString());
                    m.put("label", r.get("label").asString());
                    m.put("type", r.get("type").asString());
                    m.put("layer", r.get("layer").asObject());
                    m.put("domain", r.get("domain").asObject());
                    m.put("kind", r.get("kind").asObject());
                    m.put("dbType", r.get("dbType").asObject());
                    m.put("subscriber", r.get("subscriber").asObject());
                    m.put("syncMode", r.get("syncMode").asObject());
                    nodes.add(m);
                }
                var er = tx.run("MATCH ()-[r]->() RETURN type(r) AS t, r.sourceField AS sf, r.targetField AS tf, r.transform AS tr, startNode(r).id AS from, endNode(r).id AS to");
                while (er.hasNext()) {
                    Record r = er.next();
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("type", r.get("t").asString());
                    m.put("from", r.get("from").asString());
                    m.put("to", r.get("to").asString());
                    m.put("sourceField", r.get("sf").asObject());
                    m.put("targetField", r.get("tf").asObject());
                    m.put("transform", r.get("tr").asObject());
                    edges.add(m);
                }
                return null;
            });
        } catch (Exception e) {
            throw new BizException(ErrorCode.PARAM_ERROR, "血缘图查询失败: " + e.getMessage());
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nodes", nodes);
        out.put("edges", edges);
        return out;
    }
}
