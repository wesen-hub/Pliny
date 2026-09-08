package com.dcp.module.system;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.dcp.common.ApiResponse;
import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.system.entity.SysRole;
import com.dcp.module.system.entity.SysUser;
import com.dcp.module.system.entity.SysUserRole;
import com.dcp.module.system.entity.SysPermission;
import com.dcp.module.system.entity.SysRolePermission;
import com.dcp.module.system.mapper.SysPermissionMapper;
import com.dcp.module.system.mapper.SysRoleMapper;
import com.dcp.module.system.mapper.SysRolePermissionMapper;
import com.dcp.module.system.mapper.SysUserMapper;
import com.dcp.module.system.mapper.SysUserRoleMapper;
import com.dcp.module.system.service.AuditService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class SystemController {

    private final SysUserMapper userMapper;
    private final SysRoleMapper roleMapper;
    private final SysUserRoleMapper userRoleMapper;
    private final SysPermissionMapper permissionMapper;
    private final SysRolePermissionMapper rolePermissionMapper;
    private final com.dcp.security.PermissionService permService;
    private final PasswordEncoder passwordEncoder;
    private final AuditService auditService;

    // ---------- 用户管理 ----------

    @Data
    public static class UserRequest {
        private Long id;
        private String username;
        private String password;
        private String nickname;
        private String email;
        private String phone;
        private Integer status;
        private List<String> roles;
    }

    // ---------- 角色权限管理(P1 RBAC) ----------

    @Data
    public static class RoleRequest {
        private String roleCode;
        private String roleName;
        private String description;
    }

    @GetMapping("/permissions")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<List<Map<String, Object>>> permissions() {
        List<SysPermission> all = permissionMapper.selectList(
                new LambdaQueryWrapper<SysPermission>().orderByAsc(SysPermission::getModule, SysPermission::getCode));
        Map<String, List<Map<String, Object>>> grouped = new java.util.LinkedHashMap<>();
        for (SysPermission p : all) {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("code", p.getCode());
            m.put("name", p.getName());
            m.put("type", p.getType());
            m.put("hot", p.getHot());
            grouped.computeIfAbsent(p.getModule(), k -> new java.util.ArrayList<>()).add(m);
        }
        List<Map<String, Object>> out = new java.util.ArrayList<>();
        grouped.forEach((module, perms) -> out.add(Map.of("module", module, "perms", perms)));
        return ApiResponse.ok(out);
    }

    @PostMapping("/roles")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> createRole(@RequestBody RoleRequest req) {
        if (req.getRoleCode() == null || req.getRoleCode().isBlank()) throw new BizException(ErrorCode.PARAM_ERROR, "角色 code 必填");
        String code = req.getRoleCode().trim().toUpperCase();
        SysRole exist = roleMapper.selectOne(new LambdaQueryWrapper<SysRole>().eq(SysRole::getRoleCode, code));
        if (exist != null) throw new BizException(ErrorCode.PARAM_ERROR, "角色已存在: " + code);
        SysRole r = new SysRole();
        r.setRoleCode(code);
        r.setRoleName(req.getRoleName() == null ? code : req.getRoleName());
        r.setDescription(req.getDescription());
        roleMapper.insert(r);
        return ApiResponse.ok(Map.of("id", r.getId(), "roleCode", code));
    }

    @PutMapping("/roles/{id}")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> updateRole(@PathVariable Long id, @RequestBody RoleRequest req) {
        SysRole r = roleMapper.selectById(id);
        if (r == null) throw new BizException(ErrorCode.NOT_FOUND, "角色不存在");
        if (req.getRoleName() != null) r.setRoleName(req.getRoleName());
        if (req.getDescription() != null) r.setDescription(req.getDescription());
        roleMapper.updateById(r);
        return ApiResponse.ok(Map.of("id", id));
    }

    @DeleteMapping("/roles/{id}")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> deleteRole(@PathVariable Long id) {
        SysRole r = roleMapper.selectById(id);
        if (r == null) throw new BizException(ErrorCode.NOT_FOUND, "角色不存在");
        if (List.of("ADMIN", "OPERATOR", "VIEWER", "AUDITOR", "LABEL_ENG", "ANALYST").contains(r.getRoleCode())) {
            throw new BizException(ErrorCode.PARAM_ERROR, "预置角色不可删除");
        }
        rolePermissionMapper.delete(new LambdaQueryWrapper<SysRolePermission>().eq(SysRolePermission::getRoleId, id));
        roleMapper.deleteById(id);
        permService.refresh();
        return ApiResponse.ok(Map.of("deleted", true, "id", id));
    }

    /** 角色已授权权限码 */
    @GetMapping("/roles/{id}/perms")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Set<String>> rolePerms(@PathVariable Long id) {
        return ApiResponse.ok(permService.permsOfRole(id));
    }

    /** 保存角色权限点(全量覆盖) */
    @PutMapping("/roles/{id}/perms")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> saveRolePerms(@PathVariable Long id, @RequestBody List<String> codes) {
        SysRole r = roleMapper.selectById(id);
        if (r == null) throw new BizException(ErrorCode.NOT_FOUND, "角色不存在");
        if ("ADMIN".equals(r.getRoleCode())) throw new BizException(ErrorCode.PARAM_ERROR, "ADMIN 拥有全部权限, 无需配置");
        rolePermissionMapper.delete(new LambdaQueryWrapper<SysRolePermission>().eq(SysRolePermission::getRoleId, id));
        for (String c : codes) {
            SysRolePermission rp = new SysRolePermission();
            rp.setRoleId(id);
            rp.setPermCode(c);
            rolePermissionMapper.insert(rp);
        }
        permService.refresh();
        auditService.record("system", "ROLE_PERM_SAVE", "role", id, "保存角色权限: " + r.getRoleCode());
        return ApiResponse.ok(Map.of("id", id, "count", codes.size()));
    }

    @GetMapping("/users")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> users(@RequestParam(defaultValue = "1") int page,
                                                  @RequestParam(defaultValue = "10") int pageSize,
                                                  @RequestParam(required = false) String keyword) {
        LambdaQueryWrapper<SysUser> qw = new LambdaQueryWrapper<>();
        qw.like(keyword != null && !keyword.isBlank(), SysUser::getUsername, keyword)
          .orderByDesc(SysUser::getId);
        Page<SysUser> p = userMapper.selectPage(Page.of(page, pageSize), qw);
        List<Map<String, Object>> list = p.getRecords().stream().map(u -> {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("id", u.getId());
            m.put("username", u.getUsername());
            m.put("nickname", u.getNickname());
            m.put("email", u.getEmail());
            m.put("phone", u.getPhone());
            m.put("status", u.getStatus());
            m.put("createTime", u.getCreateTime());
            m.put("roles", userMapper.selectRolesByUserId(u.getId()).stream().map(SysRole::getRoleCode).toList());
            return m;
        }).toList();
        return ApiResponse.ok(Map.of("total", p.getTotal(), "list", list));
    }

    @PostMapping("/users")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Long> createUser(@RequestBody UserRequest req) {
        if (req.getUsername() == null || req.getUsername().isBlank()
                || req.getPassword() == null || req.getPassword().isBlank()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "用户名和密码必填");
        }
        if (userMapper.selectCount(new LambdaQueryWrapper<SysUser>()
                .eq(SysUser::getUsername, req.getUsername())) > 0) {
            throw new BizException(ErrorCode.PARAM_ERROR, "用户名已存在");
        }
        SysUser user = new SysUser();
        user.setUsername(req.getUsername());
        user.setPasswordHash(passwordEncoder.encode(req.getPassword()));
        user.setNickname(req.getNickname());
        user.setEmail(req.getEmail());
        user.setPhone(req.getPhone());
        user.setStatus(req.getStatus() == null ? 1 : req.getStatus());
        userMapper.insert(user);
        saveRoles(user.getId(), req.getRoles());
        auditService.record("system", "CREATE", "user", user.getId(), "创建用户: " + req.getUsername());
        return ApiResponse.ok(user.getId());
    }

    @PutMapping("/users/{id}")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Void> updateUser(@PathVariable Long id, @RequestBody UserRequest req) {
        SysUser user = userMapper.selectById(id);
        if (user == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "用户不存在");
        }
        if (req.getPassword() != null && !req.getPassword().isBlank()) {
            user.setPasswordHash(passwordEncoder.encode(req.getPassword()));
        }
        if (req.getNickname() != null) user.setNickname(req.getNickname());
        if (req.getEmail() != null) user.setEmail(req.getEmail());
        if (req.getPhone() != null) user.setPhone(req.getPhone());
        if (req.getStatus() != null) user.setStatus(req.getStatus());
        userMapper.updateById(user);
        if (req.getRoles() != null) {
            saveRoles(id, req.getRoles());
        }
        auditService.record("system", "UPDATE", "user", id, "更新用户: " + user.getUsername());
        return ApiResponse.ok();
    }

    @DeleteMapping("/users/{id}")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Void> deleteUser(@PathVariable Long id) {
        userMapper.deleteById(id);
        userRoleMapper.delete(new LambdaQueryWrapper<SysUserRole>().eq(SysUserRole::getUserId, id));
        auditService.record("system", "DELETE", "user", id, "删除用户");
        return ApiResponse.ok();
    }

    private void saveRoles(Long userId, List<String> roles) {
        userRoleMapper.delete(new LambdaQueryWrapper<SysUserRole>().eq(SysUserRole::getUserId, userId));
        if (roles != null) {
            for (String code : roles) {
                SysRole role = roleMapper.selectOne(new LambdaQueryWrapper<SysRole>().eq(SysRole::getRoleCode, code));
                if (role != null) {
                    SysUserRole ur = new SysUserRole();
                    ur.setUserId(userId);
                    ur.setRoleId(role.getId());
                    userRoleMapper.insert(ur);
                }
            }
        }
    }

    // ---------- 角色 ----------

    @GetMapping("/roles")
    public ApiResponse<List<SysRole>> roles() {
        return ApiResponse.ok(roleMapper.selectList(null));
    }

    // ---------- 审计 ----------

    @GetMapping("/audit-logs")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> auditLogs(@RequestParam(defaultValue = "1") int page,
                                                      @RequestParam(defaultValue = "10") int pageSize,
                                                      @RequestParam(required = false) String module,
                                                      @RequestParam(required = false) String username,
                                                      @RequestParam(required = false) String operation) {
        var p = auditService.page(page, pageSize, module, username, operation);
        return ApiResponse.ok(Map.of("total", p.getTotal(), "list", p.getRecords()));
    }
}
