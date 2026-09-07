package com.dcp.module.job.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("field_mapping")
public class FieldMapping {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long jobId;
    private String sourceField;
    private String targetField;
    /** direct|dictionary|expression */
    private String transformType;
    private String transformRule;
    private Integer sortNo;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
