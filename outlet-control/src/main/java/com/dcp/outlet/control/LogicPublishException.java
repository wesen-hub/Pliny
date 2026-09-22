package com.dcp.outlet.control;

/**
 * 逻辑发布被拒绝(错误前置:发布动作把可发布性问题在落地前拦下,而非带到运行时)。
 *
 * <p>典型场景:同版本重复发布(禁止覆盖)、试加载预检失败、声明不一致。
 * 对应平台错误码语义 {@code LOGIC_PUBLISH_REJECTED}。
 */
public class LogicPublishException extends RuntimeException {

    public LogicPublishException(String message) {
        super(message);
    }

    public LogicPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
