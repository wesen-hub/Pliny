package com.dcp.outlet.rt;

/** 消息解析端口(生产实现 = canal-json,只取需要的字段)。 */
public interface MessageDecoder {

    DecodedMessage decode(RawMessage raw);
}
