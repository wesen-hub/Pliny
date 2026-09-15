package com.dcp.outlet.sdk;

/** 找不到指定 {@code logicId@version} —— 对应平台错误码 {@code LOGIC_NOT_AVAILABLE}。 */
public class LogicNotFoundException extends SdkException {

    private final LogicRef ref;

    public LogicNotFoundException(LogicRef ref) {
        super("逻辑未注册:" + ref + "(检查是否部署到 worker / 版本号是否写错)");
        this.ref = ref;
    }

    public LogicRef ref() { return ref; }
}
