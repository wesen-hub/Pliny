package com.dcp.outlet.sdk;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明一段消费逻辑(D14/D26)。
 *
 * <p><b>注意</b>:业务参数**不在这里声明** —— 业务参数由任务类型的垂直模块负责定义与校验
 * (ADR-9 / D28);平台内核只把 {@code params} 原样透传给逻辑。
 *
 * <pre>{@code
 * @ConsumerLogicDef(logicId = "es-sync", version = "1.0.0")
 * public class EsSyncLogic implements ConsumerLogic { ... }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ConsumerLogicDef {

    /** 逻辑标识,如 {@code es-sync}。同 id 可有多版本。 */
    String logicId();

    /** 逻辑版本,如 {@code 1.0.0}。任务按 {@code logicId@version} 绑定。 */
    String version();

    /** 可读描述(选填,供平台展示)。 */
    String description() default "";
}
