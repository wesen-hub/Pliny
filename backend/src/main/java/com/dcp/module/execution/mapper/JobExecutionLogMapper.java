package com.dcp.module.execution.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dcp.module.execution.entity.JobExecutionLog;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface JobExecutionLogMapper extends BaseMapper<JobExecutionLog> {
}
