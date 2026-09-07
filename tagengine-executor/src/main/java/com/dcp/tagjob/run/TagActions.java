package com.dcp.tagjob.run;

import org.kie.api.runtime.KieSession;

/**
 * 动作约定实现(用户 DRL 的 API, 见 DrlGenerator 注释与模块 README):
 * DRL 包内 function 声明包装调用本类静态方法; sink/session 用 ThreadLocal,
 * 在每次 fire 前 bind(当前行上下文 + 当前会话), 行处理完 clear —— 每个 executor 任务线程各一份, 无跨线程泄漏。
 */
public final class TagActions {
    private static final ThreadLocal<RowSink> SINK = new ThreadLocal<>();
    private static final ThreadLocal<KieSession> SESSION = new ThreadLocal<>();

    private TagActions() {
    }

    /** fire 前绑定行级 sink(记标签输出/命中计数/halt 标志) */
    public static void bindSink(RowSink sink) {
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
        RowSink sink = SINK.get();
        if (sink == null) return;   // 防御: 未 bind 时静默(正常路径总是 bind)
        sink.tag(path, value);
    }

    /** halt(): 停止当前资产组求值 —— 置标志 + 尽力中断当前 fire(与 drools.halt() 同效); 作用于当前执行对象 */
    public static void halt() {
        RowSink sink = SINK.get();
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
}
