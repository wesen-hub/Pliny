package com.dcp.scheduler.exec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * SPI 插件声明(微服务架构方案 §8.1):标注在业务 jar 的 Factory 实现类上,
 * 提供稳定的插件名(替代实现类简单名,后者易随类名重构漂移)。
 * TaskSpec.handlerRef.ref 与 @ExecutorPlugin.value() 必须一致。
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ExecutorPlugin {

    /** 插件名(框架按此名装载,EMBEDDED 注册名同义)。 */
    String value();
}
