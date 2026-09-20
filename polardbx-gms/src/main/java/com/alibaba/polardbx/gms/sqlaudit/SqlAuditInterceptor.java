package com.alibaba.polardbx.gms.sqlaudit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.ast.SqlType;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SqlAuditInterceptor {
    private static String USER = "user";
    private static String SCHEMA_NAME = "schema_name";
    private static String OPERATION = "operation";

    //key: user-schemaName, value: auditAction set
    // if user is '*", record schemaName -> value
    private static Map<String, Set<SqlAuditAction>> auditMap = new ConcurrentHashMap<>();

    // operations: [*]
    private static Map<String, Boolean> operationWhiteMap = new ConcurrentHashMap<>();

    private static Map<String, Boolean> loginSuccessPerm = new ConcurrentHashMap<>();

    private static Map<String, Boolean> loginFailedPerm = new ConcurrentHashMap<>();

    private static Map<String, Boolean> logoutPerm = new ConcurrentHashMap<>();

    public synchronized static void updateAuditLogConfig(String config) {
        // Parse the JSON string
        try {
            JSONObject jsonObject = JSON.parseObject(config);

            loginSuccessPerm.clear();
            loginFailedPerm.clear();
            logoutPerm.clear();
            operationWhiteMap.clear();
            auditMap.clear();
            if (jsonObject == null) {
                return;
            }
            // Iterate over the keys in the JSON object (rule names)
            for (String ruleName : jsonObject.keySet()) {
                JSONObject rule = jsonObject.getJSONObject(ruleName);
                JSONArray users = rule.getJSONArray(USER);
                JSONArray schemaNames = rule.getJSONArray(SCHEMA_NAME);
                JSONArray operations = rule.getJSONArray(OPERATION);
                // Nested loops to cover all permutations
                for (int i = 0; i < users.size(); i++) {
                    String user = users.getString(i);
                    for (int j = 0; j < schemaNames.size(); j++) {
                        String schemaName = schemaNames.getString(j);
                        String key = getKey(user, schemaName);
                        Set<SqlAuditAction> actions = new HashSet<>();
                        for (int k = 0; k < operations.size(); k++) {
                            String operation = operations.getString(k);
                            if (operation.trim().equalsIgnoreCase(SqlAuditAction.LOGIN_SUCCESS.name())) {
                                loginSuccessPerm.put(user, true);
                            }
                            if (operation.trim().equalsIgnoreCase(SqlAuditAction.LOGIN_FAILED.name())) {
                                loginFailedPerm.put(user, true);
                            }
                            if (operation.trim().equalsIgnoreCase(SqlAuditAction.LOGOUT.name())) {
                                logoutPerm.put(user, true);
                            }
                            if ("*".equals(operation.trim())) {
                                if ("*".equals(schemaName.trim())) {
                                    operationWhiteMap.put(user, true);
                                    // user = *, schema = *, operation = "*"
                                    if ("*".equals(user.trim())) {
                                        operationWhiteMap.put("*", true);
                                        return;
                                    }
                                } else if ("*".equals(user.trim())) {
                                    operationWhiteMap.put(schemaName, true);
                                } else {
                                    operationWhiteMap.put(key, true);
                                }
                            }
                            SqlAuditAction action = SqlAuditAction.value(operation);
                            actions.add(action);
                        }
                        if ("*".equals(user.trim())) {
                            auditMap.put(schemaName, actions);
                        } else if ("*".equals(schemaName.trim())) {
                            auditMap.put(user, actions);
                        } else {
                            auditMap.put(key, actions);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public static boolean hasLoginSuccessPerm(String user) {
        if (!DynamicConfig.getInstance().getEnableSqlAudit()) {
            // record login in default
            return true;
        }
        return loginSuccessPerm.containsKey(user) || operationWhiteMap.containsKey(user)
            || operationWhiteMap.containsKey("*");
    }

    public static boolean hasLoginFailedPerm(String user) {
        if (!DynamicConfig.getInstance().getEnableSqlAudit()) {
            return true;
        }
        return loginFailedPerm.containsKey(user) || operationWhiteMap.containsKey(user)
            || operationWhiteMap.containsKey("*");
    }

    public static boolean hasLogoutPerm(String user) {
        if (!DynamicConfig.getInstance().getEnableSqlAudit()) {
            // record logout in default
            return true;
        }
        return logoutPerm.containsKey(user) || operationWhiteMap.containsKey(user)
            || operationWhiteMap.containsKey("*");
    }

    public static boolean hasPermission(String user, String schemaName, SqlType sqlType) {
        if (!DynamicConfig.getInstance().getEnableSqlAudit()) {
            return false;
        }
        //not set
        if (auditMap.isEmpty() || operationWhiteMap.containsKey("*")) {
            return true;
        }
        String key = getKey(user, schemaName);
        if (operationWhiteMap.containsKey(key) || operationWhiteMap.containsKey(schemaName)
            || operationWhiteMap.containsKey(user)) {
            return true;
        }
        return hasPermissionCell(key, sqlType) || hasPermissionCell(user, sqlType) || hasPermissionCell(schemaName,
            sqlType);
    }

    private static boolean hasPermissionCell(String key, SqlType sqlType) {
        Set<SqlAuditAction> actionSet = auditMap.get(key);
        return actionSet != null && checkContains(actionSet, sqlType);
    }

    public static boolean checkContains(Set<SqlAuditAction> actionSet, SqlType sqlType) {
        SqlAuditAction action = SqlAuditAction.convert(sqlType);
        return action != null && actionSet.contains(action);
    }

    private static String getKey(String user, String schemaName) {
        return user + '-' + schemaName;
    }


    public static void validateJsonValue(String relVal) throws Exception {
        try {
            JSONObject json = JSON.parseObject(relVal);

            for (String ruleKey : json.keySet()) {
                JSONObject rule = json.getJSONObject(ruleKey);

                if (!rule.containsKey("user") || !rule.containsKey("schema_name") || !rule.containsKey(
                    "operation")) {
                    throw new RuntimeException(ruleKey
                        + "' missing necessary field（user, schema_name, operation）");
                }

                if (!(rule.get("user") instanceof JSONArray) || !(rule.get("schema_name") instanceof JSONArray)
                    || !(rule.get("operation") instanceof JSONArray)) {
                    throw new RuntimeException(
                        ruleKey + " (user, schema_name, operation) must be JSONArray ");
                }

                JSONArray userArray = rule.getJSONArray("user");
                JSONArray schemaNameArray = rule.getJSONArray("schema_name");
                JSONArray operationArray = rule.getJSONArray("operation");

                if (userArray.isEmpty() || schemaNameArray.isEmpty() || operationArray.isEmpty()) {
                    throw new RuntimeException(
                        ruleKey + " (user, schema_name, operation) must be not empty ");
                }

                // Validate each operation in the operation array
                for (int i = 0; i < operationArray.size(); i++) {
                    String operation = operationArray.getString(i);
                    if (operation == null || (SqlAuditAction.value(operation) == null && !operation.equals("*"))) {
                        throw new RuntimeException(
                            ruleKey + " contains invalid operation: '" + operation);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
