package com.dcp.module.datasource.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("datasource_config")
public class DatasourceConfig {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String name;
    /** mysql|postgresql|oracle|sqlserver|mongodb */
    private String type;
    private String host;
    private Integer port;
    private String databaseName;
    private String username;
    /** 密文: base64(IV||CT||TAG)，接口永远不返回明文 */
    @JsonIgnore
    private String passwordCiphertext;
    private Integer keyVersion;
    private String extraParams;
    private String groupName;
    private String env;
    private Integer enabled;
    private LocalDateTime lastHealthCheckTime;
    private String lastHealthCheckResult;
    private String createBy;
    private String updateBy;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private Integer deleted;
}
