# dcp-tag-kie —— 打标编译库(三端共用)

打标引擎的 Drools 编译栈唯一实现。backend(在线)、dcp-tagstream(流)、tagengine-executor(批)
三个工程共用本库,不再各自拷贝(收敛前 6 个类三处拷贝且已分叉)。

## 内容

| 类 | 职责 |
|---|---|
| `RuleAsset` | 资产(规则/规则流)统一最小形状接口(id/kind/name/ver/def);三端各自的规则快照类实现它 |
| `DrlGenerator` | 资产定义 → DRL 文本(可视化/决策表/手写 DRL 三种 mode) |
| `FactSourceGen` | 动态 Fact 类 Java 源码生成(包 `dcp.tagjob.fact.t{id}_v{ver}`,简单名 `Fact`) |
| `KieBaseFactory` | Fact 源码 + 资产 DRL → KieModule 编译(KieBuilder 标准姿势) |
| `AssetPlan` / `BuildPlan` | 编译产物/执行计划模型(driver ↔ executor 广播) |
| `TagActions` | DRL 动作实现(`tag()`/`halt()`,ThreadLocal sink/session) |

## 约定(改动前必读)

- **DRL 头硬编码 `com.dcp.tag.kie.TagActions` FQCN**(见 `DrlGenerator.header`):生成的 DRL 里的
  `tag()`/`halt()` 都指向这里。改包名/类名必须同步 header,且三个调用方的运行 jar 都必须含本库。
- **生成期错误 fail-closed**:`genRuleDrl` 结尾统一检查 errors,非空即返回 null(不静默生成)。
  因此字符串字段的数值决策表、非法操作符等都会在生成期被拒 —— 与后端发布校验语义一致。
- 动态 Fact 的包名仍是 `dcp.tagjob.fact.*`(生成代码,非编译期依赖),不必随本库迁移。

## 依赖与打包

- drools 8.44.1.Final(engine/core/compiler/mvel)+ jackson-databind 2.17.2,均为 compile。
- 本模块不 shade;由调用方(executor/tagstream 的 shade 插件、backend 的 boot jar)打进运行 jar。
- 三端构建前先 `mvn install` 本模块(独立 Maven 工程,无父 pom)。

## 调用方接入方式

1. pom 加依赖 `com.dcp:dcp-tag-kie:1.0.0`;
2. 各自规则快照类 `implements RuleAsset`(字段与方法同名合法,保留 public 字段即可);
3. 删除本地 `DrlGenerator/FactSourceGen/KieBaseFactory/AssetPlan/BuildPlan/TagActions` 拷贝;
4. 引用改 `import com.dcp.tag.kie.*`;行级 sink 走 `TagActions.Sink`(批执行器的 RowSink 实现之),
   在线/流用内置 `TagActions.Collector`(含 `halted()/resetHalt()`)。

## 本地编译

```bash
cd ~/Desktop/data-collect-platform
mvn -f dcp-tag-kie/pom.xml install
```
