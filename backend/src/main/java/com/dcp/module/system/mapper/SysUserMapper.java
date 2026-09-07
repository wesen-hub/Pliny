package com.dcp.module.system.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dcp.module.system.entity.SysRole;
import com.dcp.module.system.entity.SysUser;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface SysUserMapper extends BaseMapper<SysUser> {

    @Select("""
            SELECT r.* FROM sys_role r
            JOIN sys_user_role ur ON ur.role_id = r.id
            WHERE ur.user_id = #{userId}
            """)
    List<SysRole> selectRolesByUserId(Long userId);
}
