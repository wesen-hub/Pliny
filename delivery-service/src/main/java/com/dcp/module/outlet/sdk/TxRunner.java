package com.dcp.module.outlet.sdk;

import java.util.function.Supplier;

/**
 * 事务执行器:让"业务写 + 位点推进"落在**同一个事务**里(设计 §6.8:"位点推进 ⟺ 数据已落地")。
 *
 * <p>跨库场景(位点库与业务库不同)无法同事务 —— 此时 SDK 走"先写数据、后推进位点"的顺序,
 * 失效重放由幂等写入吸收。
 */
public interface TxRunner {

    <T> T inTx(Supplier<T> work);
}
