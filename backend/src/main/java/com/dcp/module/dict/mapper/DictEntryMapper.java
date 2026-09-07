package com.dcp.module.dict.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dcp.module.dict.entity.DictEntry;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface DictEntryMapper extends BaseMapper<DictEntry> {
}
