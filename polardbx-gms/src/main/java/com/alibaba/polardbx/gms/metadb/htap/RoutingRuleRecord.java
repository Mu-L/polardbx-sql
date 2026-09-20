package com.alibaba.polardbx.gms.metadb.htap;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RoutingRuleRecord implements SystemTableRecord {
    public long id;
    public String instId;
    public String ruleName;
    public String userName;
    public String templateId;
    public List<String> keywords;
    public String routingType;
    public Timestamp createdTime;

    @Override
    public RoutingRuleRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.ruleName = rs.getString("rule_name");
        this.userName = rs.getString("user_name");
        this.templateId = rs.getString("template_id");
        this.keywords = deserializeKeyWords(rs.getString("keywords"));
        this.routingType = rs.getString("routing_type");
        this.createdTime = rs.getTimestamp("gmt_created");
        return this;
    }

    protected Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(6);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.instId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.ruleName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.userName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.templateId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, serializeKeyWords(this.keywords));
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.routingType);
        return params;
    }

    private String serializeKeyWords(List<String> keywords) {
        if (keywords == null) {
            return null;
        }
        return JSON.toJSONString(keywords);
    }

    private List<String> deserializeKeyWords(String keyword) {
        if (StringUtils.isEmpty(keyword)) {
            return null;
        }
        return JSON.parseArray(keyword, String.class);
    }
}
