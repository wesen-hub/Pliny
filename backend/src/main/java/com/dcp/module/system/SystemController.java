package com.dcp.module.system;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.dcp.common.ApiResponse;
import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.system.entity.SysRole;
import com.dcp.module.system.entity.SysUser;
import com.dcp.module.system.entity.SysUserRole;
import com.dcp.module.system.mapper.SysRoleMapper;
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

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class SystemController {

    private final SysUserMapper userMapper;
    private final SysRoleMapper roleMapper;
    private final SysUserRoleMapper userRoleMapper;
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
