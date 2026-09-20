package com.alibaba.polardbx.gms.locality;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbConfig {
    public Map<String, List<String>> getGroup_config() {
        return group_config;
    }

    public void setGroup_config(Map<String, List<String>> group_config) {
        this.group_config = group_config;
    }

    public Map<String, List<String>> group_config = new HashMap<>();

    @JSONCreator
    public DbConfig(Map<String, List<String>> group_config) {
        this.group_config = group_config;
    }

    public Boolean checkValidate() {
        if (GeneralUtil.isEmpty(this.group_config)) {
            return false;
        }
        for (String dbAlias : this.group_config.keySet()) {
            if (group_config.get(dbAlias).size() != 2) {
                return false;
            }
        }
        return true;
    }
}
