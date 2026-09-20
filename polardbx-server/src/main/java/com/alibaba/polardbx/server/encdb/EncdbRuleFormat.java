package com.alibaba.polardbx.server.encdb;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRule;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.gms.metadb.encdb.mask.EncdbMaskAlgo;
import com.alibaba.polardbx.gms.metadb.encdb.mask.EncdbMaskType;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.json.JsonObject;
import com.alibaba.polardbx.server.encdb.handler.EncdbRuleVersion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.ALGO;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.COLUMNS;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.DATABASES;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.DESCRIPTION;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.ENABLED;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.FULL_ACCESS;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.META;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.NAME;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.PARAMS;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.RESTRICTED_ACCESS;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.RULES;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.TABLES;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.TYPE;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.USERS;
import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.VERSION;
import static com.alibaba.polardbx.gms.metadb.encdb.EncdbRule.jsonArr2Set;
import static com.alibaba.polardbx.gms.metadb.encdb.EncdbRule.set2JsonArr;

public class EncdbRuleFormat {

    public static List<EncdbRule> parseNewEncRule(JSONObject encRule) {
        int version = encRule.getIntValue(VERSION);
        if (version != EncdbRuleVersion.VERSION_1.getVersion()) {
            throw new EncdbException("the rule format is invalid");
        }
        JSONArray rules = encRule.getJSONArray(RULES);
        List<EncdbRule> encdbRules = new ArrayList<>(rules.size());
        for (int i = 0; i < rules.size(); i++) {
            JSONObject ruleJson = rules.getJSONObject(i);
            String ruleName = ruleJson.getString(NAME);
            boolean enable = ruleJson.getBoolean(ENABLED);
            String description = ruleJson.getString(DESCRIPTION);

            JSONObject meta = ruleJson.getJSONObject(META);
            Set<String> dbs = jsonArr2Set(meta.getJSONArray(DATABASES));
            Set<String> tbs = jsonArr2Set(meta.getJSONArray(TABLES));
            Set<String> cols = jsonArr2Set(meta.getJSONArray(COLUMNS));

            JSONObject users = ruleJson.getJSONObject(USERS);
            Set<PolarAccount> fullAccessUsers = users.getJSONArray(FULL_ACCESS)
                    .stream().map(s -> PolarAccount.fromIdentifier((String) s))
                    .collect(Collectors.toSet());
            Set<PolarAccount> restrictedAccessUsers = users.getJSONArray(RESTRICTED_ACCESS)
                    .stream().map(s -> PolarAccount.fromIdentifier((String) s))
                    .collect(Collectors.toSet());

            String ruleTypeStr = ruleJson.getString(TYPE);
            EncdbRule.EncdbRuleType ruleType =
                    ruleTypeStr == null ? EncdbRule.EncdbRuleType.ENCRYPTION : EncdbRule.EncdbRuleType.valueOf(ruleTypeStr.toUpperCase());

            EncdbMaskAlgo maskAlgo = null;
            if (ruleType == EncdbRule.EncdbRuleType.MASKING) {
                //默认按照数据类型进行掩码
                maskAlgo = new EncdbMaskAlgo(EncdbMaskType.MASK_DATA_TYPE, new Object[0]);
                if (ruleJson.containsKey(ALGO)) {
                    JSONObject maskAlgoJson = ruleJson.getJSONObject(ALGO);
                    EncdbMaskType maskType = EncdbMaskType.valueOf(maskAlgoJson.getString(TYPE).toUpperCase());
                    Object[] params = maskAlgoJson.getJSONArray(PARAMS).toArray();
                    maskAlgo = EncdbMaskAlgo.buildEncdbMaskAlgo(maskType, params);
                }
            }

            encdbRules.add(
                    new EncdbRule(ruleName, enable, fullAccessUsers, restrictedAccessUsers, dbs, tbs, cols, description,
                            ruleType, maskAlgo)
            );
        }

        return encdbRules;
    }

    public static List<EncdbRule> parseExistEncRule(JSONObject encRule) {
        int version = encRule.getIntValue(VERSION);
        if (version != EncdbRuleVersion.VERSION_1.getVersion()) {
            throw new EncdbException("the rule format is invalid");
        }
        JSONArray rules = encRule.getJSONArray(RULES);
        List<EncdbRule> encdbRules = new ArrayList<>(rules.size());
        for (int i = 0; i < rules.size(); i++) {
            JSONObject ruleJson = rules.getJSONObject(i);
            String ruleName = ruleJson.getString(NAME);
            EncdbRule rule = EncdbRuleManager.getInstance().getEncRule(ruleName);
            if (rule == null){
                throw new EncdbException(ErrorCode.ERR_ENCDB, "encdb rule not exist : " + ruleName);
            }
            rule = rule.clone();
            Boolean enable = ruleJson.getBoolean(ENABLED);
            if (enable != null) {
                rule.setEnable(enable);
            }

            String description = ruleJson.getString(DESCRIPTION);
            if (description != null) {
                rule.setDescription(description);
            }

            JSONObject meta = ruleJson.getJSONObject(META);
            if (meta != null) {
                if (meta.containsKey(DATABASES)) {
                    Set<String> dbs = jsonArr2Set(meta.getJSONArray(DATABASES));
                    rule.setDbs(dbs);
                }
                if (meta.containsKey(TABLES)) {
                    Set<String> tbs = jsonArr2Set(meta.getJSONArray(TABLES));
                    rule.setTbs(tbs);
                }
                if (meta.containsKey(COLUMNS)) {
                    Set<String> cols = jsonArr2Set(meta.getJSONArray(COLUMNS));
                    rule.setCols(cols);
                }
            }

            JSONObject users = ruleJson.getJSONObject(USERS);
            if (users != null) {
                if (users.containsKey(FULL_ACCESS)) {
                    Set<PolarAccount> fullAccessUsers = users.getJSONArray(FULL_ACCESS)
                            .stream().map(s -> PolarAccount.fromIdentifier((String) s))
                            .collect(Collectors.toSet());
                    rule.setFullAccessUsers(fullAccessUsers);
                }
                if (users.containsKey(RESTRICTED_ACCESS)) {
                    Set<PolarAccount> restrictedAccessUsers = users.getJSONArray(RESTRICTED_ACCESS)
                            .stream().map(s -> PolarAccount.fromIdentifier((String) s))
                            .collect(Collectors.toSet());
                    rule.setRestrictedAccessUsers(restrictedAccessUsers);
                }
            }

            String ruleTypeStr = ruleJson.getString(TYPE);
            if (ruleTypeStr != null) {
                EncdbRule.EncdbRuleType ruleType = EncdbRule.EncdbRuleType.valueOf(ruleTypeStr.toUpperCase());
                if (ruleType != rule.getRuleType()) {
                    throw new EncdbException(ErrorCode.ERR_ENCDB, "encdb rule type can not be modified");
                }
            }

            if (ruleJson.containsKey(ALGO)) {
                if (rule.getRuleType() == EncdbRule.EncdbRuleType.MASKING){
                    JSONObject maskAlgoJson = ruleJson.getJSONObject(ALGO);
                    EncdbMaskType maskType = EncdbMaskType.valueOf(maskAlgoJson.getString(TYPE).toUpperCase());
                    Object[] params = maskAlgoJson.getJSONArray(PARAMS).toArray();
                    EncdbMaskAlgo maskAlgo = EncdbMaskAlgo.buildEncdbMaskAlgo(maskType, params);
                    rule.setMaskAlgo(maskAlgo);
                }
            }
            encdbRules.add(rule);
        }
        return encdbRules;
    }


    public static JSONObject getEncdbRuleJson(List<EncdbRule> encdbRules, EncdbRuleVersion ruleVersion) {
        if (ruleVersion != EncdbRuleVersion.VERSION_1){
            throw new EncdbException("the rule format is invalid");
        }

        JSONObject encRule = new JSONObject();
        encRule.put(VERSION, ruleVersion.getVersion());
        JSONArray rules = new JSONArray();
        encRule.put(RULES, rules);
        for (EncdbRule encdbRule : encdbRules) {
            JSONObject ruleJson = new JSONObject();
            ruleJson.put(NAME, encdbRule.getRuleName());
            ruleJson.put(ENABLED, encdbRule.isEnable());
            ruleJson.put(TYPE, encdbRule.getRuleType().name().toLowerCase());
            ruleJson.put(DESCRIPTION, encdbRule.getDescription());

            JSONObject ruleMeta = new JSONObject();
            ruleJson.put(META, ruleMeta);
            ruleMeta.put(DATABASES, set2JsonArr(encdbRule.getDbs()));
            ruleMeta.put(TABLES, set2JsonArr(encdbRule.getTbs()));
            ruleMeta.put(COLUMNS, set2JsonArr(encdbRule.getCols()));

            JSONObject users = new JSONObject();
            ruleJson.put(USERS, users);
            users.put(FULL_ACCESS,
                    new JSONArray(encdbRule.getFullAccessUsers().stream().map(PolarAccount::getIdentifierWithoutQuote)
                            .collect(Collectors.toList())));
            users.put(RESTRICTED_ACCESS,
                    new JSONArray(encdbRule.getRestrictedAccessUsers().stream().map(PolarAccount::getIdentifierWithoutQuote)
                            .collect(Collectors.toList())));

            if (encdbRule.getRuleType() == EncdbRule.EncdbRuleType.MASKING){
                JSONObject maskAlgo = new JSONObject();
                maskAlgo.put(TYPE, encdbRule.getMaskAlgo().getMaskType().toString());
                maskAlgo.put(PARAMS, new JSONArray(Arrays.asList(encdbRule.getMaskAlgo().getParams())));
                ruleJson.put(ALGO, maskAlgo);
            }
            rules.add(ruleJson);
        }
        return encRule;
    }

}
