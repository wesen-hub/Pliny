package com.dcp.outlet.sdk;

/**
 * 消费逻辑(D26):你只需要实现这一个接口。
 *
 * <p><b>三方法生命周期</b>(平台保证调用时机):
 * <ol>
 *   <li>{@link #init(LogicInitContext)} —— 每单元一次,建连接池 / 加载规则缓存 / 编译表达式</li>
 *   <li>{@link #process(Batch, ConsumerContext)} —— 每批一次,<b>单线程串行</b>调用**同一实例**</li>
 *   <li>{@link #close()} —— 停机一次,刷缓冲 / 释放资源</li>
 * </ol>
 *
 * <p><b>三条契约</b>(违反后果自负):
 * <ol>
 *   <li><b>幂等</b> —— 平台是"至少一次投递",同一批可能重复;按业务键 upsert</li>
 *   <li><b>失败要抛</b> —— 出错必须抛异常,平台据此"不提交 → 重试 → 死信";
 *       吞异常 = 静默丢数</li>
 *   <li><b>不开线程池</b> —— 需要并发请提高任务的 {@code instances} 或加分区</li>
 * </ol>
 *
 * <p><b>提交不用你管</b>:{@code process()} 正常返回后平台自动 {@code commitSync()};
 * 仅异步 / 攒批场景才用 {@link ConsumerContext#commit()}。
 *
 * <pre>{@code
 * @ConsumerLogicDef(logicId = "es-sync", version = "1.0.0")
 * public class EsSyncLogic implements ConsumerLogic {
 *     private RestHighLevelClient es;
 *
 *     @Override public void init(LogicInitContext ctx) {
 *         this.es = new RestHighLevelClient(RestClient.builder(
 *                 HttpHost.create((String) ctx.params().get("esUrl"))));
 *     }
 *
 *     @Override public void process(Batch batch, ConsumerContext ctx) {
 *         for (ChangeRecord r : batch.records()) { ... }   // 幂等写入;失败抛异常
 *     }
 *
 *     @Override public void close() { es.close(); }
 * }
 * }</pre>
 */
public interface ConsumerLogic {

    /** 单元启动时调用一次。默认空实现。 */
    default void init(LogicInitContext ctx) {
    }

    /** 每批调用一次。单线程串行,同一实例。 */
    void process(Batch batch, ConsumerContext ctx);

    /** 单元停机时调用一次。默认空实现。 */
    default void close() {
    }
}
