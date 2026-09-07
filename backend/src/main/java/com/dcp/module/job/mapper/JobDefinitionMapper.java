package com.dcp.module.job.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dcp.module.job.entity.JobDefinition;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface JobDefinitionMapper extends BaseMapper<JobDefinition> {
}
