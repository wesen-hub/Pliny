package com.dcp.module.outlet;

import com.dcp.module.outlet.model.ConsumerAccount;
import com.dcp.module.outlet.model.Contract;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 消费方与契约注册表(设计 §7.5)。
 *
 * <p><b>骨架实现为内存版</b>,字段与《表结构DDL-2026-09-14.sql》的 {@code consumer}/{@code contract} 表一一对应;
 * 落地时把本类替换为 Mapper 版即可,**上层服务不感知**。
 */
@Slf4j
@Component
public class OutletRegistry {

    private final Map<String, ConsumerAccount> consumers = new LinkedHashMap<>();
    private final Map<String, Contract> contracts = new LinkedHashMap<>();

    @PostConstruct
    public void seed() {
        // 消费方 ①:对外订阅供数(通道 api;列白名单 + 行条件 + 密级上限 + 脱敏)
        ConsumerAccount sub = new ConsumerAccount();
        sub.setConsumerId("demo-sub");
        sub.setName("演示订阅方");
        sub.setConsumerType(ConsumerAccount.TYPE_SUBSCRIBE);
        sub.setExecutor(ConsumerAccount.EXEC_STREAM);
        sub.setChannel("api");
        sub.setSubjectId("role_data_viewer");
        consumers.put(sub.getConsumerId(), sub);

        // 消费方 ②:内部打标任务(走出口,不豁免;内部契约自动审批)
        ConsumerAccount tag = new ConsumerAccount();
        tag.setConsumerId("demo-tag");
        tag.setName("演示打标任务");
        tag.setConsumerType(ConsumerAccount.TYPE_TAG);
        tag.setExecutor(ConsumerAccount.EXEC_STREAM);
        tag.setChannel("internal");
        tag.setSubjectId("role_data_engineer");
        tag.setQuotaQps(50);
        consumers.put(tag.getConsumerId(), tag);

        // 契约 ①:列白名单 id/name/amount/phone;行条件 region='CN';密级上限 2;phone 脱敏
        //          (region 不在白名单 → 验“越权列连 key 都不出现”;rowCondition 仍按源行判定)
        Contract c1 = new Contract();
        c1.setContractId("ct-sub-t_order");
        c1.setConsumerId("demo-sub");
        c1.setStreams(Set.of("t_order"));
        c1.setColumnWhitelist(Map.of("t_order", Set.of("id", "name", "amount", "phone")));
        c1.setRowCondition("region = CN");
        c1.setMaxLevel(2);
        c1.setMaskRules(Map.of("phone", "MASK_PHONE"));
        c1.setChannel("api");
        c1.setStartPolicy("snapshot");
        c1.setExpireAt(LocalDateTime.now().plusDays(7));
        contracts.put(c1.getContractId(), c1);

        // 契约 ②:内部打标,列更宽(含 region),无脱敏
        Contract c2 = new Contract();
        c2.setContractId("ct-tag-t_order");
        c2.setConsumerId("demo-tag");
        c2.setStreams(Set.of("t_order"));
        c2.setColumnWhitelist(Map.of("t_order", Set.of("id", "name", "amount", "region")));
        c2.setRowCondition(null);
        c2.setMaxLevel(3);
        c2.setChannel("internal");
        c2.setStartPolicy("snapshot");
        contracts.put(c2.getContractId(), c2);

        log.info("[outlet] 注册表已seed:{} 个消费方 / {} 个契约", consumers.size(), contracts.size());
    }

    public ConsumerAccount consumer(String consumerId) {
        return consumers.get(consumerId);
    }

    public Contract contract(String contractId) {
        return contracts.get(contractId);
    }

    /** 落库版实现:select * from contract where consumer_id=? and status='active' */
    public Contract activeContractOf(String consumerId) {
        return contracts.values().stream()
                .filter(c -> c.getConsumerId().equals(consumerId) && c.active())
                .findFirst().orElse(null);
    }

    public Map<String, Object> snapshotForDebug() {
        Map<String, Object> m = new HashMap<>();
        m.put("consumers", consumers.values());
        m.put("contracts", contracts.values());
        return m;
    }
}
