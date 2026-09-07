package com.dcp.module.dict.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("dict_entry")
public class DictEntry {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long dictId;
    private String sourceValue;
    private String targetValue;
    private Integer sortNo;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
