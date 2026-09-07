package com.dcp.agent;

import java.util.Map;

/** 入口: 注册(注册码 → agentId+secret)→ 启动引擎 */
public final class AgentMain {

    public static void main(String[] args) throws Exception {
        AgentConfig cfg = new AgentConfig();
        Store store = new Store(cfg.dataDir);
        PlatformClient client = new PlatformClient(cfg);

        Map<String, Object> cred = store.credentials();
        long agentId;
        String secret;
        if (cred.containsKey("agentId")) {
            agentId = ((Number) cred.get("agentId")).longValue();
            secret = String.valueOf(cred.get("secret"));
            log("已有凭据 agentId=" + agentId + ", 跳过注册");
        } else {
            if (cfg.registerCode == null || cfg.registerCode.isBlank()) {
                System.err.println("首次运行必须提供注册码: 环境变量 DCP_AGENT_REGISTER_CODE");
                System.exit(2);
            }
            Map<String, Object> r = client.register(cfg.registerCode);
            agentId = ((Number) r.get("agentId")).longValue();
            secret = String.valueOf(r.get("secret"));
            store.saveCredentials(agentId, secret);
            log("注册成功 agentId=" + agentId + " name=" + r.get("name"));
        }

        SyncEngine engine = new SyncEngine(cfg, store, client);
        engine.setCredentials(agentId, secret);
        engine.start();
        log("dcp-agent 运行中(Ctrl+C 退出)");
        Thread.currentThread().join();
    }

    private static void log(String s) {
        System.out.println("[" + java.time.LocalTime.now().withNano(0) + "] " + s);
    }
}
