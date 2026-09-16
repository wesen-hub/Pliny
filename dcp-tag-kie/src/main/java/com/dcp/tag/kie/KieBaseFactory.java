package com.dcp.tag.kie;

import org.drools.compiler.kie.builder.impl.KieServicesImpl;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.builder.Results;

import java.util.List;
import java.util.logging.Logger;

/**
 * KieBase 编译工厂(driver 校验编译 + executor 本地重建共用; 设计文档 §12)。
 *
 * 关键点(Drools 标准姿势):
 *  - 动态 Fact 类以 <b>Java 源码写入 KieModule(src/main/java)</b>, 与资产 DRL 一起交给 KieBuilder
 *    编译 —— 不再使用运行时自定义 loader define 的类(实测 Drools import 解析对自定义 loader 不可靠),
 *    也不再依赖 javax.tools 内存编译(该方案连同 MemoryJavaCompiler 已废弃删除)。
 *  - 编译成功后返回 {@link KieBuildResult}: KieBase + moduleClassLoader(KieContainer.getClassLoader());
 *    Fact 实例化/反射 setter 表一律基于 moduleClassLoader 加载的类 —— 与 Drools 内部同一类加载器,
 *    无需任何 TCCL 黑客。
 *  - 每个资产生成独立包 → 同一 KieBase 内多资产共存不冲突(包隔离; agenda 按包名过滤)。
 *  - 动作约定以 DRL function 声明实现(见 DrlGenerator.header), 调用静态 TagActions。
 */
public final class KieBaseFactory {

    private static final Logger LOG = Logger.getLogger("dcp-tag-kie");

    private KieBaseFactory() {
    }

    /** 获取 KieServices: 优先标准 Factory; shade 单 jar 若 kie.conf 服务发现失败则直接实例化兜底 */
    public static KieServices kieServices() {
        try {
            return KieServices.Factory.get();
        } catch (Throwable t) {
            LOG.warning(() -> "KieServices.Factory.get() 失败(" + t + "), 直接 new KieServicesImpl() 兜底");
            return new KieServicesImpl();
        }
    }

    /**
     * 编译「Fact Java 源码 + 一组资产 DRL」→ KieModule(一并入 KieBuilder 编译)。
     * 失败返回 null 并把编译错误明细(含定位, [编译错误] path 行)追加到 errs; 成功返回结果对象。
     *
     * @param assets         资产 DRL 列表(AssetPlan.drl, 空跳过); 至少一个非空否则报错
     * @param factJavaSource Fact 类 Java 源码(public 类, 类名须与 factFqcn 末段一致)
     * @param factFqcn       Fact 类全限定名(如 dcp.tagjob.fact.t1_v2.Fact); 包路径决定源码写入位置
     * @param errs           编译错误明细收集(调用方可空)
     */
    public static KieBuildResult buildBase(List<AssetPlan> assets, String factJavaSource,
                                           String factFqcn, List<String> errs) {
        KieServices ks = kieServices();
        KieFileSystem kfs = ks.newKieFileSystem();

        // Fact Java 源 → src/main/java/<pkg 路径>/Fact.java(KieBuilder 一并编译, 类进入 moduleClassLoader)
        if (factJavaSource == null || factJavaSource.isBlank()) {
            errs.add("Fact Java 源码为空(factJavaSource 未生成?)");
            return null;
        }
        String factPath = javaSourcePath(factFqcn);
        kfs.write(factPath, factJavaSource);

        // 每资产 DRL → src/main/resources/... 
        int written = 0;
        for (AssetPlan a : assets) {
            if (a.drl == null || a.drl.isEmpty()) continue;
            kfs.write("src/main/resources/dcp/assets/a" + a.id + "_v" + a.ver + ".drl", a.drl);
            written++;
        }
        if (written == 0) {
            errs.add("无任何可编译的 DRL(绑定规则/流为空)");
            return null;
        }

        KieBuilder builder = ks.newKieBuilder(kfs).buildAll();
        Results results = builder.getResults();
        boolean hasError = false;
        if (results != null) {
            for (Message m : results.getMessages(Message.Level.ERROR)) {
                hasError = true;
                errs.add("  [编译错误] " + m.getPath() + " " + m.getText() + " (行 " + m.getLine() + ")");
            }
        }
        if (hasError) return null;
        try {
            var container = ks.newKieContainer(builder.getKieModule().getReleaseId());
            return new KieBuildResult(container.getKieBase(), container.getClassLoader());
        } catch (Throwable t) {
            errs.add("KieBase 装配失败: " + t);
            return null;
        }
    }

    /** driver 侧编译校验(只校验, 不保留): 编译错误 → errs 明细; 无错返回 true */
    public static boolean validateOnly(List<AssetPlan> assets, String factJavaSource,
                                       String factFqcn, List<String> errs) {
        return buildBase(assets, factJavaSource, factFqcn, errs) != null;
    }

    /** FQCN → KieModule 内 Java 源路径: dcp.tagjob.fact.t1_v2.Fact → src/main/java/dcp/tagjob/fact/t1_v2/Fact.java */
    public static String javaSourcePath(String factFqcn) {
        int dot = factFqcn.lastIndexOf('.');
        String pkg = dot < 0 ? "" : factFqcn.substring(0, dot);
        String simple = dot < 0 ? factFqcn : factFqcn.substring(dot + 1);
        return "src/main/java/" + pkg.replace('.', '/') + "/" + simple + ".java";
    }

    /** KieModule 编译结果: KieBase(会话求值用) + moduleClassLoader(Fact 类反射用, 与 KieBase 内部同源) */
    public record KieBuildResult(KieBase kieBase, ClassLoader moduleClassLoader) {
    }
}
