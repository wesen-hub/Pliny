package com.dcp.module.outlet.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dcp.module.outlet.entity.OutletConsumerEntity;
import org.apache.ibatis.annotations.Mapper;

/** 消费方服务账号 Mapper(DDL 的 consumer 表) */
@Mapper
public interface OutletConsumerMapper extends BaseMapper<OutletConsumerEntity> {
}
