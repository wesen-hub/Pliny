package com.dcp.outlet.sdk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 默认内存实现:worker 侧使用;平台会把注册结果连同心跳一起上报。 */
public class InMemoryLogicRegistry implements LogicRegistry {

    private final Map<LogicRef, ConsumerLogic> logics = new ConcurrentHashMap<>();

    @Override
    public void register(LogicRef ref, ConsumerLogic logic) {
        logics.put(ref, logic);
    }

    @Override
    public Optional<ConsumerLogic> find(LogicRef ref) {
        return Optional.ofNullable(logics.get(ref));
    }

    @Override
    public List<LogicRef> list() {
        return new ArrayList<>(logics.keySet());
    }
}
