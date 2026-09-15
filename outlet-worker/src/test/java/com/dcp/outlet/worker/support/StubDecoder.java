package com.dcp.outlet.worker.support;

import com.dcp.outlet.worker.DecodedMessage;
import com.dcp.outlet.worker.MessageDecoder;
import com.dcp.outlet.worker.RawMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 按 offset 查表的解码器(测试用)。 */
public class StubDecoder implements MessageDecoder {

    private final Map<Long, DecodedMessage> byOffset = new ConcurrentHashMap<>();

    public StubDecoder put(long offset, DecodedMessage msg) {
        byOffset.put(offset, msg);
        return this;
    }

    @Override
    public DecodedMessage decode(RawMessage raw) {
        DecodedMessage d = byOffset.get(raw.offset());
        if (d == null) throw new IllegalStateException("no scripted message for offset " + raw.offset());
        return d;
    }
}
