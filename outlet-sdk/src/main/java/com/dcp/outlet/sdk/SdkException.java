package com.dcp.outlet.sdk;

/** SDK 通用异常。逻辑抛出的任何异常都会让平台"不提交位点"并走重试 / 死信。 */
public class SdkException extends RuntimeException {

    public SdkException(String message) { super(message); }

    public SdkException(String message, Throwable cause) { super(message, cause); }
}
