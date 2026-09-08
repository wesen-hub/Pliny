package com.dcp.config;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.dcp.module.system.entity.SysPermission;
import com.dcp.module.system.entity.SysRole;
import com.dcp.module.system.entity.SysRolePermission;
import com.dcp.module.system.entity.SysUser;
import com.dcp.module.system.entity.SysUserRole;
import com.dcp.module.system.mapper.SysPermissionMapper;
import com.dcp.module.system.mapper.SysRoleMapper;
import com.dcp.module.system.mapper.SysRolePermissionMapper;
import com.dcp.module.system.mapper.SysUserMapper;
import com.dcp.module.system.mapper.SysUserRoleMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 权限体系种子(幂等, 每项单独检查; 2026-09-08 权限落地 P1):
 * 权限点清单 / 预置角色 LABEL_ENG·ANALYST / 演示用户 zhang(标签工程师)·li(分析师) / 六角色权限映射。
 */
@Slf4j
@Component
@Order(20)
@RequiredArgsConstructor
public class PermSeedInitializer implements CommandLineRunner {

    private final SysPermissionMapper permMapper;
    private final SysRoleMapper roleMapper;
    private final SysRolePermissionMapper rolePermMapper;
    private final SysUserMapper userMapper;
    private final SysUserRoleMapper userRoleMapper;
    private final PasswordEncoder passwordEncoder;
    private final com.dcp.security.PermissionService permService;

    /** 权限点: code -> {name, module, type, hot} */
    private static final Map<String, String[]> PERMS = new LinkedHashMap<>();
    /** 角色 -> 权限码列表(空=全部; ADMIN 特殊处理) */
    private static final Map<String, List<String>> ROLE_PERMS = new LinkedHashMap<>();

    static {
        perm("datasource:page", "数据源管理页", "平台运维", "menu", 0);
        perm("datasource:write", "数据源写", "平台运维", "action", 0);
        perm("datasource:test", "连通性测试", "平台运维", "action", 0);
        perm("sync:task:page", "采集/同步/湖任务页", "平台运维", "menu", 0);
        perm("sync:task:run", "触发运行/重跑", "平台运维", "action", 0);
        perm("sync:task:delete", "删除任务", "平台运维", "action", 1);
        perm("ingest:page", "Agent/文件同步/事件接入页", "平台运维", "menu", 0);
        perm("agent:control", "启停 Agent", "平台运维", "action", 1);
        perm("ingest:app:manage", "appKey 管理", "平台运维", "action", 0);
        perm("asset:page", "资产目录/血缘页", "资产", "menu", 0);
        perm("asset:secret:write", "资产定级/密级修改", "资产", "action", 1);
        perm("tag:registry:page", "标签注册表页", "打标V2", "menu", 0);
        perm("tag:registry:write", "注册/修改标签声明", "打标V2", "action", 0);
        perm("tag:rule:page", "规则库页", "打标V2", "menu", 0);
        perm("tag:rule:write", "规则/规则流写", "打标V2", "action", 0);
        perm("tag:task:page", "打标任务页", "打标V2", "menu", 0);
        perm("tag:task:write", "任务创建/编辑", "打标V2", "action", 0);
        perm("tag:rule:publish", "发布(固化快照+认领)", "打标V2", "action", 1);
        perm("tag:task:run", "触发运行/全量重算", "打标V2", "action", 1);
        perm("tag:stream:control", "流/在线作业启停", "打标V2", "action", 1);
        perm("tag:task:delete", "删除任务", "打标V2", "action", 1);
        perm("tag:sub:page", "标签订阅页", "打标V2", "menu", 0);
        perm("tag:sub:manage", "订阅管理(数据出口)", "打标V2", "action", 0);
        perm("label:query:page", "标签服务(查询/圈选)页", "标签消费", "menu", 0);
        perm("label:export", "导出实体明细", "标签消费", "action", 1);
        perm("system:page", "系统管理页", "系统", "menu", 0);
        perm("system:user:manage", "用户管理/角色分配", "系统", "action", 1);
        perm("system:role:manage", "角色权限管理", "系统", "action", 1);
        perm("system:data-policy:manage", "数据权限配置", "系统", "action", 1);
        perm("audit:read", "审计日志查看", "系统", "action", 0);
        perm("sync:task:write", "同步/湖/文件任务创建编辑", "平台运维", "action", 0);
        perm("job:write", "采集任务写(建/改/删/配置)", "平台运维", "action", 0);
        perm("job:run", "采集任务启停/运行", "平台运维", "action", 1);
        perm("dict:write", "字典写", "平台运维", "action", 0);
        perm("alarm:write", "告警规则写", "平台运维", "action", 0);

        ROLE_PERMS.put("ADMIN", List.of());   // 空=全部
        ROLE_PERMS.put("OPERATOR", List.of("datasource:page", "datasource:write", "datasource:test",
                "sync:task:page", "sync:task:write", "sync:task:run", "sync:task:delete", "ingest:page", "agent:control",
                "ingest:app:manage", "asset:page", "asset:secret:write", "job:write", "job:run",
                "dict:write", "alarm:write", "system:page", "audit:read"));
        ROLE_PERMS.put("LABEL_ENG", List.of("asset:page", "tag:registry:page", "tag:registry:write",
                "tag:rule:page", "tag:rule:write", "tag:task:page", "tag:task:write", "tag:rule:publish",
                "tag:task:run", "tag:stream:control", "tag:task:delete", "tag:sub:page", "tag:sub:manage",
                "label:query:page", "label:export", "system:page", "audit:read"));
        ROLE_PERMS.put("ANALYST", List.of("asset:page", "tag:sub:page", "tag:sub:manage", "label:query:page"));
        ROLE_PERMS.put("VIEWER", List.of("datasource:page", "sync:task:page", "ingest:page", "asset:page",
                "tag:registry:page", "tag:rule:page", "tag:task:page", "tag:sub:page", "label:query:page"));
        ROLE_PERMS.put("AUDITOR", List.of("asset:page", "system:page", "audit:read"));
    }

    private static void perm(String code, String name, String module, String type, int hot) {
        PERMS.put(code, new String[]{name, module, type, String.valueOf(hot)});
    }

    @Override
    public void run(String... args) {
        try {
            seedPerms();
            seedRoles();
            seedRolePerms();
            seedUsers();
            permService.refresh();
            log.info("[perm] 权限种子就绪(权限点 {} 个, 角色映射 {} 个)", PERMS.size(), ROLE_PERMS.size());
        } catch (Exception e) {
            log.error("[perm] 权限种子初始化失败: {}", e.getMessage(), e);
        }
    }

    private void seedPerms() {
        for (Map.Entry<String, String[]> e : PERMS.entrySet()) {
            Long n = permMapper.selectCount(new LambdaQueryWrapper<SysPermission>().eq(SysPermission::getCode, e.getKey()));
            if (n != null && n > 0) continue;   // 增量: 缺失码补插
            SysPermission p = new SysPermission();
            p.setCode(e.getKey());
            p.setName(e.getValue()[0]);
            p.setModule(e.getValue()[1]);
            p.setType(e.getValue()[2]);
            p.setHot(Integer.parseInt(e.getValue()[3]));
            permMapper.insert(p);
        }
    }

    private SysRole roleOf(String code) {
        return roleMapper.selectOne(new LambdaQueryWrapper<SysRole>().eq(SysRole::getRoleCode, code));
    }

    private void seedRoles() {
        for (String code : List.of("LABEL_ENG", "ANALYST")) {
            if (roleOf(code) == null) {
                SysRole r = new SysRole();
                r.setRoleCode(code);
                r.setRoleName("LABEL_ENG".equals(code) ? "标签工程师" : "分析师");
                r.setDescription("LABEL_ENG".equals(code) ? "打标生产: 注册/规则/发布/启停/订阅" : "标签消费: 查询/圈选/订阅(受数据权限约束)");
                roleMapper.insert(r);
            }
        }
    }

    private void seedRolePerms() {
        for (Map.Entry<String, List<String>> e : ROLE_PERMS.entrySet()) {
            SysRole role = roleOf(e.getKey());
            if (role == null) continue;
            if (e.getValue().isEmpty()) continue;   // ADMIN 空=全部(has() 特判)
            // 增量: 角色缺的权限码补插(不删既有, 尊重运行时手工调整)
            var exist = rolePermMapper.selectList(new LambdaQueryWrapper<SysRolePermission>()
                    .eq(SysRolePermission::getRoleId, role.getId()));
            java.util.Set<String> have = new java.util.HashSet<>();
            exist.forEach(x -> have.add(x.getPermCode()));
            for (String code : e.getValue()) {
                if (have.contains(code)) continue;
                SysRolePermission rp = new SysRolePermission();
                rp.setRoleId(role.getId());
                rp.setPermCode(code);
                rolePermMapper.insert(rp);
            }
        }
    }

    private void seedUsers() {
        seedUser("zhang", "zhang123", "张工(标签工程师)", "LABEL_ENG");
        seedUser("li", "li123", "李工(分析师)", "ANALYST");
    }

    private void seedUser(String username, String rawPwd, String nickname, String roleCode) {
        SysUser u = userMapper.selectOne(new LambdaQueryWrapper<SysUser>().eq(SysUser::getUsername, username));
        if (u != null) return;
        SysUser nu = new SysUser();
        nu.setUsername(username);
        nu.setPasswordHash(passwordEncoder.encode(rawPwd));
        nu.setNickname(nickname);
        nu.setStatus(1);
        userMapper.insert(nu);
        SysRole role = roleOf(roleCode);
        if (role != null) {
            SysUserRole ur = new SysUserRole();
            ur.setUserId(nu.getId());
            ur.setRoleId(role.getId());
            userRoleMapper.insert(ur);
        }
    }
}
