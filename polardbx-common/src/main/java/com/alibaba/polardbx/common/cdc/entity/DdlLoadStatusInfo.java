package com.alibaba.polardbx.common.cdc.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DdlLoadStatusInfo implements Serializable {
    private long maxDdlId;
    private long execDdlId;
    private long delayTime;
    private long delayCount;
    private String status;
    private String errorInfo;
}
