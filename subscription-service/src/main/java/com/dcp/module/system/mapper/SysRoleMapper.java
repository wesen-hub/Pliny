package com.dcp.module.system.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dcp.module.system.entity.SysRole;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface SysRoleMapper extends BaseMapper<SysRole> {

    /** H3: JWT 角色回查(与 backend 同 SQL; 本服务无 SysUserMapper, 挂在 SysRoleMapper) */
    @Select("""
            SELECT r.* FROM sys_role r
            JOIN sys_user_role ur ON ur.role_id = r.id
            WHERE ur.user_id = #{userId}
            """)
    List<SysRole> selectRolesByUserId(Long userId);
}
