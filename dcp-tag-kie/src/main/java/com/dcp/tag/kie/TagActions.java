package com.dcp.tag.kie;

import org.kie.api.runtime.KieSession;

import java.util.ArrayList;
import java.util.List;

/**
 * 动作约定实现(用户 DRL 的 API, 见 {@link DrlGenerator} 注释): 三端(在线/流/批执行器)共用 ——
 * DRL 包内 function 声明包装调用本类静态方法; sink/session 用 ThreadLocal,
 * 在每次 fire 前 bind(当前行上下文 + 当前会话), 行处理完 clear —— 每任务线程各一份, 无跨线程泄漏。
 *
 * <p>两种 sink 形态:
 * <ul>
 *   <li>批执行器: 行级 {@code RowSink}(实现 {@link Sink}, 写 tag_fact 行 + 命中计数)</li>
 *   <li>在线/流: 内置 {@link Collector}(收集 (tag, value) 列表 + halt 标志)</li>
 * </ul>
 *
 * <p>DRL 头里硬编码本类 FQCN({@code com.dcp.tag.kie.TagActions});改包名须同步
 * {@link DrlGenerator#header(String, String)}。
 */
public final class TagActions {

    /** 行级动作接收器(批执行器的 RowSink / 内置 Collector 均实现) */
    public interface Sink {
        /** 打标签(无值 = 布尔/存在即打时 value 为 null) */
        void tag(String path, String value);

        /** 请求停止当前资产组求值(halt 语义; 停止动作也算一次动作=该资产命中) */
        void requestHalt();
    }

    private static final ThreadLocal<Sink> SINK = new ThreadLocal<>();
    private static final ThreadLocal<KieSession> SESSION = new ThreadLocal<>();

    private TagActions() {
    }

    /** fire 前绑定行级 sink(记标签输出/命中计数/halt 标志) */
    public static void bindSink(Sink sink) {
        SINK.set(sink);
    }

    /** fire 前绑定当前 KieSession(halt() 尽力中断当前 fire 用) */
    public static void bindSession(KieSession session) {
        SESSION.set(session);
    }

    public static void clearThread() {
        SINK.remove();
        SESSION.remove();
    }

    /** 打标签(无值 = 布尔/存在即打) */
    public static void tag(Object fact, String path) {
        tag(fact, path, null);
    }

    /** 打标签(带值) */
    public static void tag(Object fact, String path, String value) {
        Sink sink = SINK.get();
        if (sink != null) sink.tag(path, value);
    }

    /** halt(): 停止当前资产组求值 —— 置标志 + 尽力中断当前 fire(与 drools.halt() 同效); 作用于当前执行对象 */
    public static void halt() {
        Sink sink = SINK.get();
        if (sink != null) sink.requestHalt();
        KieSession s = SESSION.get();
        if (s != null) {
            try {
                s.halt();
            } catch (Throwable ignore) {
                // 中断尽力而为; halt 标志已置, 外层 fire 返回后据此跳过后续资产/规则
            }
        }
    }

    /** 在线/流用收集器: 只收集 (tag, value) 列表 + halt 标志(无行级落库语义) */
    public static class Collector implements Sink {
        private final List<String[]> tags = new ArrayList<>();
        private boolean halted = false;

        @Override
        public void tag(String path, String value) {
            tags.add(new String[]{path, value});
        }

        @Override
        public void requestHalt() {
            halted = true;
        }

        /** halt 标志是否置位(fire 后由执行循环消费) */
        public boolean halted() {
            return halted;
        }

        /** 消费 halt 标志(fire 后调用, 供平铺跳过/流内步骤控制) */
        public void resetHalt() {
            halted = false;
        }

        public List<String[]> tags() {
            return tags;
        }
    }
}
