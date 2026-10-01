package com.dcp.subpush.channel;

/**
 * 推送通道抽象(单测可 fake 注入; 主链路失败抛异常 → 平台不提交→重试→死信)。
 */
public interface PushChannel {

    /** 推送 payload JSON; 失败抛(底座契约"失败要抛")。 */
    void send(String body) throws Exception;

    default void close() {
    }
}
