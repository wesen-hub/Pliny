# PEP 设计文档 v1.0（内部逻辑 + 使用手册）

> 版本：v1.0 ｜ 定位：PEP 完整设计说明——内置逻辑全景、适配器与管线架构、执行管线六阶段、SPI 扩展接口、业务接入使用手册
> 配套图：PEP内部逻辑分层全景.svg / 执行管线六阶段.svg / 适配器与管线调用链路.svg / PEP请求执行全链路.svg
> 相关文档：《设计文档-v1.1.md》（§2 架构、§7~§13 执行侧、§20 全链路）、《规则定义-v1.0.md》（R-B/R-S/R-R/R-A/R-F 规则组）、《缓存体系设计-v1.0.md》（L1/L2/失效细节）

---

## 0. 文档总览

| 读者 | 读什么 |
|---|---|
| 业务接入方（使用者） | §7 使用手册（六步接入 + 混合示例） |
| 框架开发者（扩展者） | §6 SPI 清单（要写哪些接口） |
| 架构评审/设计者 | §2~§5（内置逻辑、适配器管线、六阶段） |

---

## 1. PEP 定位与部署形态

### 1.1 一句话定位

**PEP（Policy Enforcement Point）= 权限执行引擎，以 SDK 形式内嵌业务进程，把 PDP 的判定产物变成引擎上的真实过滤。**

### 1.2 部署形态

```
业务进程（JVM）
├─ 业务代码
└─ PEP SDK（jar/starter，进程内）
    ├─ 接入点适配器：MyBatis 插件 / JDBC DataSource 代理 / Spark 提交器
    ├─ L1 Caffeine（进程内 5 万条）
    ├─ 单飞 / 熔断 / 版本指针 / 5s 比对线程
    └─ 对外依赖：L2 Redis（集群共享）→ PDP（远程服务）
```

### 1.3 三条铁律

| # | 铁律 | 含义 |
|---|---|---|
| 1 | **判定唯一在 PDP** | PEP 禁止本地合成 Plan/产物，只做执行与缓存；判定失败一律 DENY |
| 2 | **值不出 PEP** | bindOrder 的值只在 PEP 绑定环节出现，产物/缓存/网络全程只有模板（C1） |
| 3 | **无权威状态** | PEP 进程内只有可重建缓存态，进程可随意重启/扩缩容，启动后自动重建一切 |

---

## 2. PEP 内置逻辑全景（32 项，业务零感知）

PEP 内置逻辑分四层请求链 + 常驻后台，共 32 项，全部框架实现，用户零感知。

### 2.1 第一层：入口与拦截（进 PEP 的闸门）

| # | 模块 | 内置逻辑 |
|---|---|---|
| 1 | 接入点拦截 | MyBatis 插件（StatementHandler.prepare）/ JDBC DataSource 代理 / Spark 提交器，物理拦截全接入范围内查询 |
| 2 | 豁免检查 | @PermSkip / skip-rules 匹配 → 跳过判定（仍记审计）；ThreadLocal 幂等标记防双重拦截 |
| 3 | 元数据解析 | resourceId/action/dialect 五级解析：注解 → 注册表 → 接入方式推断 → 全局默认 → fail-closed |
| 4 | 上下文调用与校验 | 调 Provider 取 userId/roles/attrs → 校验完整性（缺 userId/roles → DENY）→ 提取异常 fail-closed |
| 5 | SQL 解析与范围检查 | JSqlParser 解析；R-S1 放行 / R-S2 拒绝（JOIN/子查询/聚合/UNION/CTE/窗口） |
| 6 | SQL 解析缓存 | MyBatis：BoundSql 模板直接作键；JDBC：字面量归一化作模板键；命中后 AST 深拷贝（R-B8） |

### 2.2 第二层：判定与缓存（拿产物的逻辑）

| # | 模块 | 内置逻辑 |
|---|---|---|
| 7 | 缓存键计算 | 六/七元指纹：hash(planVersion, dialect, resourceId, action, rolesFingerprint, stableAttrFingerprint [, subjectIdsFingerprint]) |
| 8 | L1 Caffeine | 5 万容量、30s±5s TTL 抖动、DENY 结果同缓存（防穿透）、命中后 fullKey 二次校验（碰撞视为未命中） |
| 9 | 单飞 SingleFlight | 同 key 并发只发一次 L2/PDP，其余等待（防击穿） |
| 10 | L2 Redis | 集群共享产物、版本号比对源、并发写幂等 |
| 11 | PDP RPC | AuthorizationRequest 构造 → 响应解析 → 失败即进降级决策 |
| 12 | 熔断与降级 | 持续失败触发熔断 → DENY；PDP 不可达：缓存 PERMIT 立即降 DENY、DENY 保持、未命中 DENY（绝不 TTL 续用，R-K6） |

### 2.3 第三层：产物处理（模板 → 引擎上的真实过滤）

| # | 模块 | 内置逻辑 |
|---|---|---|
| 13 | HMAC 验签 + schema 校验 | 常量时间比对重算签名、产物结构合法性——失败视同 miss 回源 |
| 14 | DENY 检查 | 红支路：拒绝对齐 + DENY 义务，不进改写链 |
| 15 | ParamBinder | bindOrder 生成（**值唯一出现点，C1**）；A 类 ?+绑定 / B 类安全字面量转义 / C 类降级转义；绑定顺序契约（权限 ? 恒在原 SQL 之后，R-B3）；Cypher 位置参数 → 命名参数 map（R-B4） |
| 16 | PredicateInjector | 权限谓词 AND 注入原 WHERE 后；无 WHERE 补 WHERE（R-B5）；列引用带表名/别名限定（R-B6） |
| 17 | ColumnPruner | visibleColumns 裁剪；SELECT * 三级展开（产物 schema → 本地缓存 → information_schema，R-S4）；掩码列替换掩码表达式 |
| 18 | 掩码列引用检测 | 掩码列被 WHERE/GROUP BY/ORDER BY/JOIN ON 引用 → 拒绝（R-S5） |

### 2.4 第四层：执行与收尾（引擎做不了的兜底 + 闭环）

| # | 模块 | 内置逻辑 |
|---|---|---|
| 19 | 引擎执行 | 改写 SQL 交引擎；Cypher 命名参数；ES client 调用 |
| 20 | ResidualProcessor | 引擎无关 AST 求值器逐行过滤；residual 过滤+LIMIT → 拒绝（R-R3）；结果集 10000 上限（R-R4）；外部查询批量（R-R6） |
| 21 | ColumnMaskApplier | 掩码后置应用 HASH/PARTIAL/REPLACE/NULL；多策略掩码取最严（R-D9）；不影响行数 |
| 22 | 标签执行 | 模式 A 实体列表绑定（<1000）；校验式圈选声明（大标签，跨库场景模式 B 已删除） |
| 23 | ObligationExecutor | AUDIT（sync 交付前入队确认 / async 尽力）；RATE_LIMIT（事后行数检查）；NOTIFY 预留 |
| 24 | 审计通道 | 内存队列 10000 → 独立线程批量 Kafka（每批 ≤100）→ perm-service；字段全集 15 项（R-A2）；队列满分档（R-A4） |

### 2.5 常驻后台（不随请求，进程内持续运转）

| # | 模块 | 内置逻辑 |
|---|---|---|
| 25 | 版本事件消费 | Kafka 独立消费者组 = 广播语义；版本号比较幂等；offset 后提交；收到即本地失效 |
| 26 | 版本比对线程 | 5s 定时 GET perm:version:latest 与 currentPlanVersion 比对，不一致即失效（失效第二道防线） |
| 27 | L1 回收 | Caffeine TTL 自然过期，旧键物理回收（不主动删除，指针前移即可） |
| 28 | 指标统计 | recordStats 缓存命中率/判定耗时/审计积压/降级熔断事件 |
| 29 | 注册表热更新 | 资源注册表/豁免规则/表 schema 本地缓存 + 定期同步 |
| 30 | SPI 运行时 | Executor 路由注册表（dialect + accessType → Executor）+ Adapter 注册表 |

---

## 3. 适配器与管线架构（谁调谁、怎么交互）

### 3.1 两层结构

```
┌─ 接入适配层（框架专属，多个）─────────────────────────┐
│ MyBatis 拦截器    JDBC 代理    Spark 提交器   Neo4j 包装 │
│    │                │              │             │      │
│    └────────────────┴──────┬───────┴─────────────┘      │
│                    统一请求模型 QueryContext             │
└────────────────────────────────────┬───────────────────┘
┌─ 执行管线层（语言无关，唯一）─────────▼─────────────────┐
│ Parser → Binder → Injector → Pruner → Execute → Residual│
│ → Obligation（六阶段，所有接入方式共用同一条）             │
└─────────────────────────────────────────────────────────┘
```

- **适配器薄**：框架差异被压扁在 extract/apply 两步翻译里（约 30 行/个）
- **管线厚**：判定/缓存/改写/审计 32 项执行语义只有一份，所有适配器共用

### 3.2 统一接口模式（三层）

```java
// ① 适配器统一接口（每框架一个薄实现）
public interface PepAdapter {
    boolean matches(Object target);              // 这个框架对象归我管吗
    QueryContext extract(Object target);         // 框架对象 → 统一请求模型
    void apply(Object target, RewriteResult r);  // 改写结果 → 写回框架对象
}

// ② 统一请求模型（所有适配器输出同一结构）
class QueryContext {
    String sql; List<Object> params;             // 原始 SQL + 参数
    AuthorizationContext ctx;                    // 上下文（Provider 产物）
    String dialect, resourceId, action;          // 五级解析结果
}

// ③ 统一执行管线（适配器按顺序调这三刀）
public interface ExecutionPipeline {
    BoundArtifact decideAndRewrite(QueryContext ctx);          // 判定+缓存+绑定+注入+投影
    Object postProcess(Object result, BoundArtifact bound);    // 后置+义务
}
```

### 3.3 MyBatis 完整调用时序（10 步）

```
业务代码
  │ ① orderMapper.queryOrders(...)
  ▼
MyBatis 框架（走到 StatementHandler.prepare）
  │ ② 调用注册的拦截器 PermInterceptor.intercept()      ← MyBatis 调适配器
  ▼
适配器（PermInterceptor）
  │ ③ 翻译成 QueryContext → pipeline.decideAndRewrite(ctx) ← 适配器调管线（第 1 次）
  │ ④ 管线返回 RewriteResult{改写 SQL, 参数映射}
  │ ⑤ 写回 BoundSql（meta.setValue delegate.boundSql）
  │ ⑥ invocation.proceed()：MyBatis 拿改写 SQL 去数据库执行
  │ ⑦ proceed() 返回结果集
  │ ⑧ pipeline.postProcess(rs, bound)                     ← 适配器调管线（第 2 次）
  │ ⑨ 管线返回过滤/掩码后的最终结果
  │ ⑩ return 给 MyBatis → 业务
```

**三句话**：
1. 方向唯一：框架调适配器 → 适配器调管线两次（执行前 decideAndRewrite / 执行后 postProcess）→ 中间夹一次 proceed() 让引擎执行改写 SQL
2. 通信靠翻译：适配器把框架对象翻成 QueryContext 给管线，管线返回 RewriteResult，适配器再翻回框架对象——管线全程不认识 MyBatis
3. 分工清楚：适配器只有 extract/apply 两个翻译动作 + 两次管线调用，判定/缓存/改写/审计全在管线内

---

## 4. 执行管线六阶段详解

```
原始 SQL ─→ ① Parser ─→ AST
上下文+产物 ─→ ② Binder ─→ BoundArtifact（值唯一出现点）
AST+bound ─→ ③ Injector ─→ 注入谓词后的 AST
投影 ─→ ④ Pruner ─→ 最终 SELECT 清单
改后 SQL ─→ ⑤ Executor ─→ 引擎结果集
residual ─→ ⑥ ResidualProcessor ─→ 过滤/掩码后结果 → 义务 → 返回
```

| 阶段 | 输入 → 输出 | 职责与规则 | 可替换 SPI |
|---|---|---|---|
| ① QueryParser | 原始 SQL → AST+元数据 | JSqlParser 解析；R-S1 放行 / R-S2 拒绝；解析缓存（模板键+AST 深拷贝）；表名→资源推断 | QueryParser（非 SQL 方言） |
| ② ParamBinder | 产物模板+上下文 → BoundArtifact | 按 ref 解析值；bindOrder 生成（值唯一出现点 C1）；A/B/C 绑定矩阵；权限 ? 恒在原 SQL 之后（R-B3）；变量解析不到 → DENY | ParamBinder（$1/命名参数） |
| ③ PredicateInjector | BoundArtifact+AST → 注入后 AST | 权限谓词 AND 注入原 WHERE 后；无 WHERE 补（R-B5）；列引用别名限定（R-B6）；ES bool.filter 合并 | PredicateInjector（DSL 注入） |
| ④ ColumnPruner | 注入后 AST+projection → 最终清单 | visibleColumns 交集裁剪；SELECT * 三级展开（R-S4）；掩码列替换掩码表达式；掩码列被引用 → 拒绝（R-S5） | ColumnPruner（ES _source） |
| ⑤ Executor | 改写 SQL+bindOrder → 结果集 | PreparedStatement 绑定执行；Cypher 位置→命名参数（R-B4）；ES client | DialectExecutor（主接口） |
| ⑥ ResidualProcessor | 结果集+residual AST → 最终结果 | 内置 AST 求值器；residual+LIMIT → 拒绝（R-R3）；10000 上限（R-R4）；外部查询批量（R-R6）；掩码兜底（最严 R-D9） | ResidualProcessor（新 AST 节点） |

**执行顺序契约（R-R2）**：pushdown 下推（含原 LIMIT）→ 引擎执行 → residual 掩码——下推缩小结果集，后置只收尾，不重不漏。

**硬禁令**：
- residual 过滤 + LIMIT 组合 → fail-closed（先 LIMIT 后过滤 = 漏数据）
- 后置结果集 > 10000 行 → fail-closed（性能灾难）
- 值只在 ② Binder 的 bindOrder 中出现，产物/缓存/网络全程只有模板（C1）

**管线方法分段**：decideAndRewrite 覆盖 ①②③④（执行前调用）；postProcess 覆盖 ⑤⑥ + 义务（执行后调用）。

---

## 5. 三种执行方式

### 5.1 谓词下推（pushdown）——主路径

PDP 翻译 AST → 方言谓词模板 → PEP 绑定注入 → 引擎一次执行，权限过滤在引擎内完成（索引过滤、结果集小）。

### 5.2 后置执行（residual）——兜底路径

方言能力不足/自定义 Renderer 未实现该节点时，谓词翻译不出来 → 引擎无关 AST 由 PEP 内置求值器在结果集上逐行过滤。掩码恒后置（掩码影响值不影响行，R-R2 安全）。

### 5.3 混合执行——常态

一个产物可同时含 pushdown（能翻译的谓词）与 residual（翻译不了的部分）：pushdown 下推缩小范围，residual 后置收尾，顺序被 R-R2 锁死。

---

## 6. 编程扩展接口（SPI 全清单）

**原则：全部有内置默认实现，不扩展就用默认。扩展 = 替换或补充钩子。**

### 6.1 必写（1 个）

```java
public interface AuthorizationContextProvider {
    AuthorizationContext extract();   // userId/roles/attrs
}
```

### 6.2 上下文渠道扩展（3 个，可选——属性增量供给）

| 接口 | 职责 | 触发场景 |
|---|---|---|
| SubjectAttributeExtractor | 主体属性（角色继承展开等），输入 userId/roles 输出 attrs | 角色在外部服务 |
| ResourceAttributeExtractor | 资源侧属性 | 资源属性按资源运行时取 |
| EnvironmentExtractor | 环境变量（时间/环境标签，判定期折叠不进指纹 R-C7） | 动态 env 判定 |

调用链：Provider（身份来源）→ 三渠道（可选增量）→ 框架装配器合并 → 校验 → 按性质路由（低基数进指纹/波动进占位符/环境判定期折叠）。渠道提取器注册为 @Component 即被自动发现调用，Provider 不用改。

### 6.3 执行链扩展（7 个，可选——按阶段替换）

DialectExecutor（新引擎执行器，主接口）/ QueryParser / ParamBinder / PredicateInjector / ColumnPruner / ResidualProcessor / ObligationExecutor（自定义义务类型）

### 6.4 辅助扩展（2 个）

ArtifactDecoder（产物自定义解码）/ AuditReporter（审计渠道，默认 Kafka）

### 6.5 表达式扩展（1 个）

CustomExpr（R-E4 白名单外函数）：`name() + evaluate(Object[])`，是"把新函数加进白名单"的唯一正道。

### 6.6 PDP 侧对照（SDK jar 同源定义）

DialectRenderer 六件套（新引擎翻译，与 Executor 配对实现）：ExprVisitor / FunctionMapper / IdentifierQuoter / TypeConverter / LiteralRenderer / PlaceholderRenderer——细粒度替换，只覆写差异件套。

**新引擎接入 = PDP 端 Renderer + PEP 端 Executor 配对实现**（Renderer 输出的占位符格式 = Executor 的绑定契约）。谓词下推翻译在 PDP（Renderer），下推执行在 PEP（Binder/Injector/Pruner），两端都留了扩展；方言特征被 R-E7 锁在 Renderer 内。

### 6.7 注册方式

```
① @Component 自动发现：Provider / 三渠道 / ObligationExecutor / AuditReporter / CustomExpr
② router.register(...)：Executor / Parser 等执行链组件
③ 策略包提交加载（PDP 侧）：DialectRenderer 六件套
```

---

## 7. 使用手册

### 7.1 使用总览：六步

```
第 1 步：pom.xml 加依赖                            ← 1 行
第 2 步：application.yml 配地址                    ← 3 行
第 3 步：写 Provider（唯一必写的代码）               ← 20 行
第 4 步：接入点确认（MyBatis 自动 / JDBC 一行注解）   ← 可选
第 5 步：管理面配策略                              ← 权限内容
第 6 步：启动验证                                  ← 自动拦截开始
```

**生效方式类比 Spring Security**：引入依赖即整体启用（自动装配拦截所有 SQL），业务提供"用户信息来源"（Provider）+ 管理面配置"谁能看什么"（策略），不需要 @Controller 式的逐类注解。

### 7.2 第一步：引入依赖

```xml
<dependency>
    <groupId>com.perm</groupId>
    <artifactId>perm-pep-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 7.3 第二步：配置地址

```yaml
perm:
  pdp: http://pdp-cluster.perm:8080    # 判定服务地址
  l2:  redis://10.0.1.10:6379          # 共享缓存地址
```

### 7.4 第三步：写 Provider（全项目唯一必写代码）

```java
@Component
public class MyAuthContextProvider implements AuthorizationContextProvider {
    @Override
    public AuthorizationContext extract() {
        LoginUser u = LoginHolder.get();   // 你自己的登录态
        return AuthorizationContext.builder()
                .userId(u.getId())
                .roles(roleService.flatRoles(u.getId()))   // 展开继承为扁平集合
                .attribute("region", u.getRegion())        // 低基数慢变属性（进指纹）
                .attribute("dept", u.getDept())
                .build();                                  // 波动属性不要放这里
    }
}
```

**三个关键约束**：
1. roles 是扁平集合——团队树、角色继承在 Provider（或 SubjectAttributeExtractor）里展开好
2. attributes 只放低基数/慢变/非 PII（进缓存指纹）；波动属性不给，需要时走占位符绑定
3. 提取异常 → fail-closed（拿不到上下文 = DENY，不猜）

### 7.5 第四步：接入点启用（生效方式因组件而异）

| 接入点 | 生效方式 | 业务操作 |
|---|---|---|
| MyBatis | **全自动** | 零操作（starter 检测到 MyBatis 自动注册 PermInterceptor，显式排序、自动禁二级缓存） |
| JDBC | **半自动** | 数据源 Bean 加一行 `@PermJdbc` 注解 |
| Spark/Flink | **手动** | 提交时调一次 `pdpClient.decide()` |

**为什么 MyBatis 能全自动、JDBC 不行**：MyBatis 有官方插件机制（InterceptorChain），starter 自动装配时挂进去；JDBC 数据源是业务自己 new 的 Bean，框架不知道哪个该管（MyBatis 底层也走 JDBC，全自动包装会双重拦截），所以需要 @PermJdbc 显式标记。

### 7.6 拦截范围声明（哪些查询拦、哪些不拦）

**默认：接入点内全量拦截（fail-closed）**——拦后判定，无策略匹配 → DENY（R-D3）。"不拦"必须显式声明。

```java
// ① 注解豁免（就近声明）
@PermSkip(reason = "订单状态字典，非敏感数据")
List<OrderStatus> listStatus();
```

```yaml
# ② 注册表豁免（集中治理）
perm:
  skip-rules:
    - matcher: "table = sys_config"
      reason: "系统配置表，无业务数据"
```

- 豁免查询仍然进审计（豁免 ≠ 隐身）
- 试点期可用 `mode: audit-only`（未声明资源 → 放行 + 全量审计观察画像），转正后切 `strict`

### 7.7 资源声明（判定元数据五级解析）

```java
// 方式 A：注解式（MyBatis 可解析 Mapper 方法注解）
@PermResource(resourceId = "sales.sales_order", action = "read.snapshot", engine = "mysql")
List<Order> queryOrders(Date startDate);
```

```yaml
# 方式 B：注册表式（JDBC 裸文本拿不到注解，走表名匹配；存量代码零改动也用它）
perm:
  resources:
    - matcher: "table = sales_order"
      resourceId: "sales.sales_order"
      action: "read.snapshot"
      engine: "mysql"
```

```sql
-- 方式 C：推断式（零声明）：表名→resourceId、SELECT→read、数据源 URL→方言
-- 推断不出的部分 → 全局默认 → 再兜不住 → fail-closed 拒绝
```

### 7.8 混合接入完整示例（MyBatis + JDBC）

**关键：两条路径分流，避开双重拦截陷阱**

```java
@Configuration
public class PermConfig {

    @Bean @Primary @ConfigurationProperties("spring.datasource")
    public DataSource rawDataSource() { return DruidDataSourceBuilder.create().build(); }

    // ① 业务裸 JDBC 用包装后的数据源（JDBC 拦截点）
    @Bean("jdbcDataSource") @PermJdbc
    public DataSource jdbcDataSource(DataSource rawDataSource) {
        return PermDataSource.wrap(rawDataSource);
    }

    // ② MyBatis 用【原数据源】，拦截交给插件做——不能包装！MyBatis 底层也走 JDBC，
    //    否则一条 MyBatis 查询被拦两次（SDK 内置 ThreadLocal 幂等标记兜底防误配）
    @Bean
    public SqlSessionFactory sqlSessionFactory(DataSource rawDataSource) {
        SqlSessionFactoryBean factory = new SqlSessionFactoryBean();
        factory.setDataSource(rawDataSource);
        Configuration config = new Configuration();
        config.setCacheEnabled(false);                        // 禁二级缓存
        config.setLocalCacheScope(LocalCacheScope.STATEMENT); // C8：防跨权限上下文串数据
        factory.setConfiguration(config);
        return factory.getObject();
    }
}
```

```java
// MyBatis 侧（注解声明 + 豁免）
public interface OrderMapper {
    @PermResource(resourceId = "sales.sales_order", action = "read.snapshot", engine = "mysql")
    List<Order> queryOrders(@Param("startDate") Date startDate);

    @PermSkip(reason = "订单状态字典，非敏感数据")
    List<OrderStatus> listStatus();
}

// JDBC 侧（注册表声明，资源见 application.yml）
@Repository
public class ExportDao {
    @Autowired @Qualifier("jdbcDataSource")
    private DataSource jdbcDataSource;   // 用包装后的数据源

    public List<Order> exportByDate(Date start, Date end) {
        String sql = "SELECT * FROM sales_order WHERE order_date BETWEEN ? AND ?";
        // 列裁剪、掩码、行谓词已在 prepareStatement 时自动改写完成
        ...
    }
}
```

**混合场景五个注意事项**：

| # | 陷阱 | 解法 |
|---|---|---|
| 1 | 双重拦截（MyBatis 底层走 JDBC） | MyBatis 用原数据源 + 插件；JDBC 用 PermDataSource；幂等标记兜底 |
| 2 | 注解适用差异 | MyBatis 用 @PermResource；JDBC 用注册表（裸文本拿不到注解） |
| 3 | count 查询被 R-S2 误杀 | 框架通用豁免：COUNT 形态（无 GROUP BY）仅注入行谓词、免列裁剪/掩码（防总数泄漏） |
| 4 | MyBatis 缓存串数据 | 禁二级缓存 + local-cache-scope=statement |
| 5 | 同表双路径声明一致性 | 注册表两条规则指向同一 resourceId，动作按 actions 维度授权 |

### 7.9 第五步：策略配置（管理面，权限内容本身）

```json
{
  "policyId": "pol_sales_region_001",
  "policyName": "区域经理查看本区域订单（含导出）",
  "effect": "GRANT",
  "subject": { "type": "role", "ids": ["regional_manager"] },
  "resource": {
    "resourceType": "TABLE",
    "resourceId": "sales.sales_order",
    "columns": ["id", "name", "amount", "region", "status"],
    "excludeColumns": ["customer_phone"],
    "visibility": "CONDITIONAL"
  },
  "actions": ["read.snapshot", "export.csv"],
  "constraints": {
    "rowExpr": "region = '${subject.region}'",
    "maskRules": [{ "column": "amount", "type": "HASH" }]
  },
  "obligations": [
    { "type": "AUDIT", "level": "detail", "sync": true },
    { "type": "RATE_LIMIT", "maxRows": 10000 }
  ]
}
```

发布后：审批 → 原子发布 → Kafka 广播 → 全网 <1s 生效，业务进程无感、无需重启。

### 7.10 第六步：启动与验证

**启动时 AutoConfiguration 按序执行**：
1. 扫描 Provider Bean → 找不到 → **启动直接失败报错**（fail-fast：不让你带着裸奔的权限系统上线）
2. 检测 MyBatis → 自动注册 PermInterceptor（显式排序 + 禁二级缓存）
3. 初始化 L1/L2/PDP 客户端/Kafka 消费者/5s 比对线程/审计线程
4. 挂上 @PermJdbc 数据源包装
5. 预热：拉取 planVersion + 资源注册表 + 表 schema
6. 启动完成 → 所有接入点 SQL 自动进入判定链

**PDP 连不上时**：不阻断启动（进程能起），但所有判定 DENY（fail-closed 降级）——安全优先。

**验证**：跑一条 SQL，看改写结果（演示台 explain）与审计日志：

```sql
-- 改前
SELECT id, name, customer_phone, amount, region, status
FROM sales_order WHERE order_date >= ?
-- 改后（张三，regional_manager，region=华东）
SELECT id, name, SHA2(amount,256) AS amount, region, status
FROM sales_order
WHERE order_date >= ? AND (`region` = ? AND `status` IN (?,?))
-- 绑定：'2026-09-01' / '华东' / 'paid' / 'pending'
```

---

## 8. fail-closed 边界清单（14 条）

| # | 边界 | 行为 |
|---|---|---|
| 1 | 上下文缺失/提取异常 | DENY |
| 2 | 元数据解析不出 | 拒绝 |
| 3 | SQL 超范围（JOIN/聚合等） | 拒绝 |
| 4 | 验签失败 | 视同 miss 回源 |
| 5 | 绑定失败（变量解析不到） | DENY |
| 6 | 产物 schema 非法 | DENY |
| 7 | 方言无法解析 | DENY |
| 8 | 熔断态 | 一律 DENY |
| 9 | PDP 不可达 | 缓存 PERMIT 立即降 DENY、未命中 DENY |
| 10 | 审计队列满（严格档） | 拒绝交付 |
| 11 | residual 过滤 + LIMIT | 拒绝 |
| 12 | 后置结果集 > 10000 行 | 拒绝 |
| 13 | 掩码列被 WHERE/ORDER BY 等引用 | 拒绝 |
| 14 | 无策略匹配（已进入判定链） | DENY（R-D3） |

**总原则**：全链路 fail-closed（R-F1）；降级只收窄，一切降级从宽松走向严格，禁止反向放行（R-F3）。

---

## 9. 设计要点速查（15 条）

1. **判定唯一在 PDP**：PEP 禁止本地合成 Plan，只执行与缓存
2. **值不出 PEP**：bindOrder 是值唯一出现点，产物/缓存/网络全程只有模板（C1）
3. **适配器薄管线厚**：框架差异压扁在 extract/apply 两步，32 项执行语义只有一份
4. **管线两刀**：执行前 decideAndRewrite（①②③④）、执行后 postProcess（⑤⑥+义务），中间夹 proceed()
5. **六阶段全可替换**：每阶段有内置默认实现 + 对应 SPI，不扩展就用默认
6. **顺序契约 R-R2**：pushdown（含原 LIMIT）→ 引擎执行 → residual 掩码
7. **两条硬禁令**：residual+LIMIT 拒绝；后置 >10000 行拒绝
8. **绑定顺序契约**：权限 ? 恒在原 SQL 所有 ? 之后（R-B3）
9. **优先参数绑定**：有绑定能力却内联 = 注入风险（R-B1）
10. **降级只收窄**：PDP 不可达 PERMIT 立即降 DENY，绝不 TTL 续用（R-K6）
11. **元数据五级解析**：注解 → 注册表 → 推断 → 全局默认 → fail-closed
12. **豁免仍审计**：@PermSkip/skip-rules 跳过判定但不隐身
13. **启动 fail-fast**：找不到 Provider 启动失败；PDP 不可达不阻断但全 DENY
14. **MyBatis 全自动/JDBC 半自动**：MyBatis 有插件机制自动挂；JDBC 数据源需 @PermJdbc 标记（防双重拦截）
15. **新引擎两端配对**：PDP 的 Renderer 与 PEP 的 Executor 配对实现，占位符格式即绑定契约

---

## 附录 A：PEP 与 PDP 职责边界

| 职责 | PDP | PEP |
|---|---|---|
| 策略匹配/冲突消解/短路 | ✅ | ✗ |
| 方言翻译（谓词下推模板） | ✅（Renderer 六件套） | ✗ |
| 判定（decision） | ✅ | ✗（只缓存结果） |
| 绑定/注入/投影 | ✗ | ✅ |
| 引擎执行/后置过滤/掩码兜底 | ✗ | ✅ |
| 义务/审计上报 | ✗ | ✅ |
| 失效感知（事件/比对） | ✅ | ✅（各自消费） |

## 附录 B：配套 SVG 速查

| 图 | 角度 |
|---|---|
| PEP内部逻辑分层全景.svg | 静态架构：四层 26 项 + 常驻后台 + SPI 全清单 |
| 执行管线六阶段.svg | 数据流：六阶段输入输出规则 + 禁令 |
| 适配器与管线调用链路.svg | 时序：MyBatis 10 步调用 |
| PEP请求执行全链路.svg | 端到端：全分支 + fail-closed 14 条 |
