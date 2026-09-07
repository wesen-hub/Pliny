package com.dcp.module.job.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("extract_config")
public class ExtractConfig {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long jobId;
    /** table|sql */
    private String sourceType;
    private String content;
    private Integer previewLimit;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
