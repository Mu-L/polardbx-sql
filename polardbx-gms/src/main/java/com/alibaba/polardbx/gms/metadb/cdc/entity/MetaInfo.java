package com.alibaba.polardbx.gms.metadb.cdc.entity;

import lombok.Data;

@Data
public class MetaInfo {
    public LogicMeta.LogicDbMeta logicDbMeta;
    public LogicMeta.LogicTableMeta logicTableMeta;
}
