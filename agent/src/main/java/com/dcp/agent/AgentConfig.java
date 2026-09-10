package com.dcp.agent;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** 运行配置: 环境变量 > 系统属性 > 默认值 */
public final class AgentConfig {

    public final String platformUrl;
    public final String registerCode;      // 首次注册用; 注册成功后可为空
    public final String rcloneBin;
    public final String dataDir;
    public final int heartbeatSec;
    public final int pollTimeoutSec;
    public final String host;
    public final String os;
    public final String arch;
    public final String version;

    public AgentConfig() {
        platformUrl = stripSlash(env("DCP_AGENT_PLATFORM_URL", "http://localhost:8080"));
        registerCode = env("DCP_AGENT_REGISTER_CODE", null);
        rcloneBin = env("DCP_AGENT_RCLONE", "rclone");
        dataDir = env("DCP_AGENT_DATA_DIR", "./data");
        heartbeatSec = intEnv("DCP_AGENT_HEARTBEAT_SEC", 30);
        pollTimeoutSec = intEnv("DCP_AGENT_POLL_TIMEOUT", 25);
        host = env("DCP_AGENT_HOST", hostname());
        os = System.getProperty("os.name", "?");
        arch = System.getProperty("os.arch", "?");
        version = "1.0.0";
    }

    private static String hostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) {
            String p = System.getProperty(k);
            return p == null || p.isBlank() ? def : p;
        }
        return v;
    }

    private static int intEnv(String k, int def) {
        try {
            return Integer.parseInt(env(k, String.valueOf(def)));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static String stripSlash(String s) {
        return s == null ? s : s.replaceAll("/+$", "");
    }

    public static String newRunId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /** 极简 JSON(避免引 jackson 处到处散 ObjectMapper) */
    public static final ObjectMapper OM = new ObjectMapper();

    static {
        OM.findAndRegisterModules();
    }
}
