package com.dcp.module.outlet.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dcp.module.outlet.entity.OutletContractEntity;
import org.apache.ibatis.annotations.Mapper;

/** 契约 Mapper(DDL 的 contract 表) */
@Mapper
public interface OutletContractMapper extends BaseMapper<OutletContractEntity> {
}
