package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Data
public class DdlLoadMetaInfoRecord implements SystemTableRecord {

    public String configKey;
    public String configValue;

    public DdlLoadMetaInfoRecord() {
    }

    public DdlLoadMetaInfoRecord(String configKey, String configValue) {
        this.configKey = configKey;
        this.configValue = configValue;
    }

    @Override
    public DdlLoadMetaInfoRecord fill(ResultSet rs) throws SQLException {
        this.configKey = rs.getString("config_key");
        this.configValue = rs.getString("config_value");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        int index = params.size();
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.configKey);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.configValue);
        return params;
    }
}
