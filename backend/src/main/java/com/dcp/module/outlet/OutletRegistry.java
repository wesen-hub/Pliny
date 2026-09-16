package com.dcp.module.outlet;

import com.dcp.module.outlet.entity.OutletConsumerEntity;
import com.dcp.module.outlet.entity.OutletContractEntity;
import com.dcp.module.outlet.mapper.OutletConsumerMapper;
import com.dcp.module.outlet.mapper.OutletContractMapper;
import com.dcp.module.outlet.model.ConsumerAccount;
import com.dcp.module.outlet.model.Contract;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;

/**
 * 消费方与契约注册表(设计 §7.5)。
 *
 * <p>两种模式,由 {@code dcp.outlet.registry} 决定:
 * <ul>
 *   <li>{@code mapper}(推荐/生产):读元库的 {@code consumer} / {@code contract} 表
 *       (DDL 见《变更流出口-表结构DDL-2026-09-14.sql》);</li>
 *   <li>{@code memory}(默认,骨架/单测):内存种子数据,字段与表一一对应,便于无库跑通。</li>
 * </ul>
 * 上层服务只依赖本类的三个查询方法,**不感知**底层实现。
 */
@Slf4j
@Component
public class OutletRegistry {

    private final OutletConsumerMapper consumerMapper;
    private final OutletContractMapper contractMapper;
    private final String mode;
    private final ObjectMapper json = new ObjectMapper();

    /** memory 模式的数据 */
    private final Map<String, ConsumerAccount> memConsumers = new LinkedHashMap<>();
    private final Map<String, Contract> memContracts = new LinkedHashMap<>();

    public OutletRegistry(@Autowired(required = false) OutletConsumerMapper consumerMapper,
                          @Autowired(required = false) OutletContractMapper contractMapper,
                          @Value("${dcp.outlet.registry:memory}") String mode) {
        this.consumerMapper = consumerMapper;
        this.contractMapper = contractMapper;
        this.mode = mode;
    }

    @PostConstruct
    public void init() {
        seedMemory();
        log.info("[outlet] 注册表模式={}(mapper 可用={}, contract mapper 可用={})",
                mode, consumerMapper != null, contractMapper != null);
        if ("mapper".equalsIgnoreCase(mode) && (consumerMapper == null || contractMapper == null)) {
            log.warn("[outlet] 配置为 mapper 模式但 Mapper 未注入 → 自动退回 memory 模式");
        }
    }

    private boolean useMapper() {
        return "mapper".equalsIgnoreCase(mode) && consumerMapper != null && contractMapper != null;
    }

    /* ------------------------------------------------------------------ 查询入口 */

    public ConsumerAccount consumer(String consumerId) {
        if (useMapper()) {
            OutletConsumerEntity e = consumerMapper.selectById(consumerId);
            if (e == null) {
                return null;
            }
            return toModel(e);
        }
        return memConsumers.get(consumerId);
    }

    public Contract contract(String contractId) {
        if (useMapper()) {
            OutletContractEntity e = contractMapper.selectById(contractId);
            return e == null ? null : toModel(e);
        }
        return memContracts.get(contractId);
    }

    /** 取该消费方当前生效的契约(status=active 且未过期);同一消费方多契约时按流覆盖匹配(未匹配到流再退回任一 active 契约) */
    public Contract activeContractOf(String consumerId) {
        return activeContractOf(consumerId, null);
    }

    /** 按 (消费方, 流) 取生效契约: 优先找 streams 覆盖该流的 active 契约; 无流参数或未覆盖时退回首个 active 契约。 */
    public Contract activeContractOf(String consumerId, String streamId) {
        if (useMapper()) {
            List<OutletContractEntity> list = contractMapper.selectList(
                    new QueryWrapper<OutletContractEntity>()
                            .eq("consumer_id", consumerId)
                            .eq("status", Contract.STATUS_ACTIVE));
            List<Contract> active = list.stream().map(this::toModel).filter(Contract::active).toList();
            if (streamId != null) {
                for (Contract c : active) if (c.covers(streamId)) return c;
            }
            return active.isEmpty() ? null : active.get(0);
        }
        List<Contract> active = memContracts.values().stream()
                .filter(c -> c.getConsumerId().equals(consumerId) && c.active()).toList();
        if (streamId != null) {
            for (Contract c : active) if (c.covers(streamId)) return c;
        }
        return active.isEmpty() ? null : active.get(0);
    }

    /* ------------------------------------------------------------------ 实体 → 模型 */

    private ConsumerAccount toModel(OutletConsumerEntity e) {
        ConsumerAccount c = new ConsumerAccount();
        c.setConsumerId(e.getConsumerId());
        c.setName(e.getName());
        c.setConsumerType(e.getConsumerType());
        c.setStatus(e.getStatus() == null ? "active" : e.getStatus());
        c.setChannel(e.getChannel() == null ? "api" : e.getChannel());
        c.setExecutor(e.getExecutor() == null ? ConsumerAccount.EXEC_STREAM : e.getExecutor());
        c.setSubjectId(e.getSubjectId());
        if (e.getQuotaQps() != null) {
            c.setQuotaQps(e.getQuotaQps());
        }
        if (e.getQuotaRowsDay() != null) {
            c.setQuotaRowsPerDay(e.getQuotaRowsDay());
        }
        if (e.getQuotaConn() != null) {
            c.setQuotaConcurrent(e.getQuotaConn());
        }
        return c;
    }

    private Contract toModel(OutletContractEntity e) {
        Contract c = new Contract();
        c.setContractId(e.getContractId());
        c.setConsumerId(e.getConsumerId());
        c.setStreams(parseStringSet(e.getStreams()));
        c.setColumnWhitelist(parseColumnWhitelist(e.getColumnWhitelist()));
        c.setRowCondition(e.getRowCondition());
        c.setMaxLevel(e.getMaxLevel() == null ? 0 : e.getMaxLevel());
        c.setMaskRules(parseStringMap(e.getMaskRules()));
        c.setChannel(e.getChannel() == null ? "api" : e.getChannel());
        c.setStartPolicy(e.getStartPolicy() == null ? "snapshot" : e.getStartPolicy());
        c.setStatus(e.getStatus() == null ? Contract.STATUS_ACTIVE : e.getStatus());
        c.setExpireAt(e.getTokenExpireAt());
        return c;
    }

    private Set<String> parseStringSet(String jsonText) {
        Set<String> out = new LinkedHashSet<>();
        if (jsonText == null || jsonText.isBlank()) {
            return out;
        }
        try {
            JsonNode n = json.readTree(jsonText);
            if (n.isArray()) {
                n.forEach(x -> out.add(x.asText()));
            } else if (n.isTextual()) {   // 兼容 "t_order,t_user" 这种写法
                for (String s : n.asText().split(",")) {
                    if (!s.isBlank()) {
                        out.add(s.trim());
                    }
                }
            }
        } catch (Exception ex) {
            log.warn("[outlet] streams JSON 解析失败: {}", jsonText);
        }
        return out;
    }

    private Map<String, Set<String>> parseColumnWhitelist(String jsonText) {
        Map<String, Set<String>> out = new LinkedHashMap<>();
        if (jsonText == null || jsonText.isBlank()) {
            return out;
        }
        try {
            JsonNode n = json.readTree(jsonText);
            n.fields().forEachRemaining(f -> {
                Set<String> cols = new LinkedHashSet<>();
                if (f.getValue().isArray()) {
                    f.getValue().forEach(x -> cols.add(x.asText()));
                }
                out.put(f.getKey(), cols);
            });
        } catch (Exception ex) {
            log.warn("[outlet] column_whitelist JSON 解析失败: {}", jsonText);
        }
        return out;
    }

    private Map<String, String> parseStringMap(String jsonText) {
        Map<String, String> out = new LinkedHashMap<>();
        if (jsonText == null || jsonText.isBlank()) {
            return out;
        }
        try {
            JsonNode n = json.readTree(jsonText);
            n.fields().forEachRemaining(f -> out.put(f.getKey(), f.getValue().asText()));
        } catch (Exception ex) {
            log.warn("[outlet] mask_rules JSON 解析失败: {}", jsonText);
        }
        return out;
    }

    /* ------------------------------------------------------------------ memory 种子 */

    private void seedMemory() {
        ConsumerAccount sub = new ConsumerAccount();
        sub.setConsumerId("demo-sub");
        sub.setName("演示订阅方");
        sub.setConsumerType(ConsumerAccount.TYPE_SUBSCRIBE);
        sub.setExecutor(ConsumerAccount.EXEC_STREAM);
        sub.setChannel("api");
        sub.setSubjectId("role_data_viewer");
        memConsumers.put(sub.getConsumerId(), sub);

        ConsumerAccount tag = new ConsumerAccount();
        tag.setConsumerId("demo-tag");
        tag.setName("演示打标任务");
        tag.setConsumerType(ConsumerAccount.TYPE_TAG);
        tag.setExecutor(ConsumerAccount.EXEC_STREAM);
        tag.setChannel("internal");
        tag.setSubjectId("role_data_engineer");
        tag.setQuotaQps(50);
        memConsumers.put(tag.getConsumerId(), tag);

        // 契约 ①:列白名单含 phone(脱敏);region 不在白名单 → 验"越权列连 key 都不出现"
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
        memContracts.put(c1.getContractId(), c1);

        // 契约 ②:内部打标,列更宽(含 region),无脱敏
        Contract c2 = new Contract();
        c2.setContractId("ct-tag-t_order");
        c2.setConsumerId("demo-tag");
        c2.setStreams(Set.of("t_order"));
        c2.setColumnWhitelist(Map.of("t_order", Set.of("id", "name", "amount", "region")));
        c2.setMaxLevel(3);
        c2.setChannel("internal");
        c2.setStartPolicy("snapshot");
        memContracts.put(c2.getContractId(), c2);
    }

    public Map<String, Object> snapshotForDebug() {
        Map<String, Object> m = new HashMap<>();
        m.put("mode", useMapper() ? "mapper" : "memory");
        m.put("consumers", useMapper() ? "见 consumer 表" : memConsumers.values());
        m.put("contracts", useMapper() ? "见 contract 表" : memContracts.values());
        return m;
    }
}
