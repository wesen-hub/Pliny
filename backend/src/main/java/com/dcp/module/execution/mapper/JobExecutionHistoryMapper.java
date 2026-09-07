package com.dcp.module.execution.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dcp.module.execution.entity.JobExecutionHistory;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface JobExecutionHistoryMapper extends BaseMapper<JobExecutionHistory> {
}
