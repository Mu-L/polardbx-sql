package com.alibaba.polardbx.gms.locality;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DbConfigParser {
    Map<String, List<String>> proxyConfig;

    public String getSpec() {
        return spec;
    }

    String spec;

    String schemaName;

    DbConfigParser(String spec, String schemaName) {
        this.spec = spec;
        this.schemaName = schemaName;
        parse();
    }

    DbConfigParser(Map<String, List<String>> proxyConfig) {
        this.proxyConfig = proxyConfig;
        unparse();
    }

    void unparse() {
        StringBuilder sb = new StringBuilder();
//            for (Map.Entry<String, List<String>> dnAndDb : proxyConfig.entrySet()) {
//                String dn = dnAndDb.getKey();
//                String db = dnAndDb.getValue();
//                sb.append(db + ":" + dn + ",");
//            }
        sb.append(LocalityDesc.DB_CONFIG_PREFIX);
        DbConfig dbConfig = new DbConfig(proxyConfig);
        sb.append(JSON.toJSONString(dbConfig));
        if (sb.length() < 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                "invalid locality specification for proxy config: " + proxyConfig);
        }
        this.spec = sb.toString();
    }

    void parse() {
        Boolean parsedOk = false;
        Map<String, List<String>> parsedProxyConfig = new HashMap<>();
        if (spec.startsWith(LocalityDesc.DB_CONFIG_PREFIX)) {
            //
            String dbConfigText = spec.substring(LocalityDesc.DB_CONFIG_PREFIX.length(), spec.length());
            DbConfig dbConfig = (DbConfig) JSON.parseObject(dbConfigText, DbConfig.class);
            parsedOk = dbConfig.checkValidate();
            if (parsedOk) {
                parsedProxyConfig = dbConfig.group_config;
            }
        } else if (spec.startsWith(LocalityDesc.DB_CONFIG_NEW_PREFIX)) {
            String dbConfigText = spec.substring(LocalityDesc.DB_CONFIG_NEW_PREFIX.length());
            Map<String, Object> config = JSON.parseObject(dbConfigText, Map.class);
            parsedOk = checkDbConfigWithLabel(config);
            Map<String, List<String>> dbConfig = expandDbConfigWithLabel(config);
            if (parsedOk) {
                parsedProxyConfig = dbConfig;
            }
        }
        if (!parsedOk || parsedProxyConfig.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                "invalid locality specification for dble config: " + spec);
        }
        this.proxyConfig = parsedProxyConfig;
    }

    private Map<String, List<String>> expandDbConfigWithLabel(Map<String, Object> config) {
        Map<String, StorageInstHaContext> storageStatusMap = StorageHaManager.getInstance().getStorageHaCtxCache();
        Map<String, String> storageInstLabelMapping = new HashMap<>();
        for (StorageInstHaContext instHaContext : storageStatusMap.values()) {
            if (instHaContext != null && ServerInstIdManager.getInstance().getInstId()
                .equalsIgnoreCase(instHaContext.getInstId()) && !instHaContext.isMetaDb()) {

                String storageInstId = instHaContext.getStorageMasterInstId();
                String storageInstLabel = instHaContext.getStorageInstLabel();
                if (StringUtils.isEmpty(storageInstLabel)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                        String.format("the storage [%s] has no labels", storageInstId));
                }
                storageInstLabelMapping.put(storageInstLabel, storageInstId);
            }
        }

        Pattern pattern = Pattern.compile("(\\w+)\\[(.*?)]");
        Pattern pattern2 = Pattern.compile("\\w+");

        Map<String, List<String>> expandedDbConfig = new HashMap<>();
        Set<String> uniqueInstIdAndDbSet = new HashSet<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            String key = entry.getKey();
            List<String> value = (List<String>) entry.getValue();
            String storageInstLabel = value.get(0);
            String physicalDb = value.get(1);

            Matcher matcher1 = pattern.matcher(key);
            Matcher matcher2 = pattern2.matcher(key);

            Matcher matcher3 = pattern.matcher(storageInstLabel);
            Matcher matcher4 = pattern2.matcher(storageInstLabel);

            Matcher matcher5 = pattern.matcher(physicalDb);
            Matcher matcher6 = pattern2.matcher(physicalDb);

            if (matcher1.matches()) {
                String groupPrefix = matcher1.group(1);
                String groupNumber = matcher1.group(2);

                List<Integer> groupNumbers = unwrapNumber(groupNumber);

                List<String> labels = new ArrayList<>();
                if (matcher3.matches()) {
                    String labelPrefix = matcher3.group(1);
                    String labelNumber = matcher3.group(2);
                    List<Integer> labelNumbers = unwrapNumber(labelNumber);
                    labels.addAll(
                        labelNumbers.stream().map(number -> labelPrefix + number).collect(Collectors.toList()));
                }

                List<String> physicalDbNames = new ArrayList<>();
                if (matcher5.matches()) {
                    String physicalDbPrefix = matcher5.group(1);
                    String physicalDbNumber = matcher5.group(2);
                    List<Integer> physicalDbNumbers = unwrapNumber(physicalDbNumber);
                    physicalDbNames.addAll(physicalDbNumbers.stream().map(number -> physicalDbPrefix + number)
                        .collect(Collectors.toList()));
                }

                if (labels.isEmpty() && physicalDbNames.isEmpty() && groupNumbers.size() != 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                        "invalid locality specification for db config: " + spec);
                }

                if ((!labels.isEmpty() && labels.size() != groupNumbers.size())
                    || (!physicalDbNames.isEmpty() && physicalDbNames.size() != groupNumbers.size())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                        "invalid locality specification for db config, groupNumbers, labelNumbers and physicalDbNumbers size not equal");
                }

                // build group names
                List<String> tmpGroupNames =
                    groupNumbers.stream().map(e -> groupPrefix + e).collect(Collectors.toList());
                List<String> groupNames = generateGroupName(tmpGroupNames, schemaName);

                for (int i = 0; i < groupNumbers.size(); ++i) {
                    String groupName = groupNames.get(i);
                    String label = labels.isEmpty() ? storageInstLabel : labels.get(i);
                    String newPhysicalDb = physicalDbNames.isEmpty() ? physicalDb : physicalDbNames.get(i);
                    String storageInstId = storageInstLabelMapping.get(label);
                    if (storageInstId == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                            "invalid locality specification for db config, storageInstLabel not found: " + label);
                    }
                    String uk = storageInstId + ":" + newPhysicalDb;
                    if (uniqueInstIdAndDbSet.contains(uk)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                            "invalid locality specification for db config, there has been duplicate physical db or group key "
                                + spec);
                    }
                    uniqueInstIdAndDbSet.add(uk);
                    expandedDbConfig.put(groupName, Arrays.asList(storageInstId, newPhysicalDb));
                }
            } else if (matcher2.matches() && matcher4.matches() && matcher6.matches()) {
                String groupName = generateGroupName(key, schemaName);
                String storageInstId = storageInstLabelMapping.get(storageInstLabel);
                if (storageInstId == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                        "invalid locality specification for db config, storageInstLabel not found: "
                            + storageInstLabel);
                }
                String uk = storageInstId + ":" + physicalDb;
                if (uniqueInstIdAndDbSet.contains(uk)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                        "invalid locality specification for db config, there has been duplicate physical db or group key "
                            + spec);
                }
                uniqueInstIdAndDbSet.add(uk);
                expandedDbConfig.put(groupName, Arrays.asList(storageInstId, physicalDb));
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                    "invalid locality specification for db config: " + spec);
            }
        }
        return expandedDbConfig;
    }

    private static List<Integer> unwrapNumber(String number) {
        List<Integer> numbers = new ArrayList<>();
        String[] parts = number.split(",");
        for (String part : parts) {
            part = part.trim();
            if (part.contains("-")) {
                String[] range = part.split("-");
                int start = Integer.parseInt(range[0]);
                int end = Integer.parseInt(range[1]);
                if (start >= end) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                        "invalid locality specification for db config, range start >= end");
                }
                for (int i = start; i <= end; i++) {
                    numbers.add(i);
                }
            } else {
                // 单个数字
                numbers.add(Integer.parseInt(part));
            }
        }

        return numbers;
    }

    private boolean checkDbConfigWithLabel(Map<String, Object> config) {
        if (GeneralUtil.isEmpty(config)) {
            return false;
        }
        for (String dbAlias : config.keySet()) {
            List<String> value = (List<String>) config.get(dbAlias);
            if (value.size() != 2) {
                return false;
            }
        }
        return true;
    }

    public static List<String> parseGroupNames(String groupConfig, String schemaName) {
        Pattern pattern = Pattern.compile("(\\w+)\\[(.*?)]");
        Matcher matcher = pattern.matcher(groupConfig);
        List<String> groupKeys = new ArrayList<>();
        if (matcher.matches()) {
            String groupPrefix = matcher.group(1);
            String number = matcher.group(2);

            List<Integer> numbers = unwrapNumber(number);
            for (int num : numbers) {
                groupKeys.add(groupPrefix + num);
            }
        } else {
            String[] groups = groupConfig.split(",");
            groupKeys = Arrays.stream(groups).map(String::trim).collect(Collectors.toList());
        }
        return generateGroupName(groupKeys, schemaName);
    }

    public static String unparseDbConfig(Map<String, List<String>> config, String schemaName) {
        boolean isNewDbConfig = true;
        if (config == null || config.isEmpty()) {
            return null;
        }

        Map<String, StorageInstHaContext> storageStatusMap = StorageHaManager.getInstance().getStorageHaCtxCache();
        Map<String, String> storageInstMapping = new HashMap<>();
        for (StorageInstHaContext instHaContext : storageStatusMap.values()) {
            if (instHaContext != null && ServerInstIdManager.getInstance().getInstId()
                .equalsIgnoreCase(instHaContext.getInstId()) && !instHaContext.isMetaDb()) {

                String storageInstId = instHaContext.getStorageMasterInstId();
                String storageInstLabel = instHaContext.getStorageInstLabel();
                storageInstMapping.put(storageInstId, storageInstLabel);
            }
        }

        Map<String, List<String>> newConfig = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : config.entrySet()) {
            String key = entry.getKey();
            List<String> value = entry.getValue();

            if (value.size() != 2) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                    "invalid locality specification for db config");
            }

            String storageInstId = value.get(0);
            String storageInstLabel = storageInstMapping.get(storageInstId);
            if (!StringUtils.isEmpty(storageInstLabel)) {
                newConfig.put(unwrapGroupName(key, schemaName), Arrays.asList(storageInstLabel, value.get(1)));
            } else {
                isNewDbConfig = false;
            }
        }
        StringBuilder sb = new StringBuilder();
        if (isNewDbConfig) {
            sb.append(LocalityDesc.DB_CONFIG_NEW_PREFIX);
            sb.append(JSON.toJSONString(newConfig));
            return sb.toString();
        } else {
            return null;
        }
    }

    public static String generateGroupName(String groupName, String schemaName) {
        return generateGroupName(Collections.singletonList(groupName), schemaName).get(0);
    }

    public static List<String> generateGroupName(List<String> groupNameList, String schemaName) {
        if (schemaName == null || schemaName.isEmpty()) {
            return groupNameList;
        }

        List<DbGroupInfoRecord> dbGroupInfoRecords =
            DbGroupInfoManager.getInstance().queryGroupInfoBySchema(schemaName);
        Set<String> groupNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (dbGroupInfoRecords != null) {
            groupNameSet.addAll(dbGroupInfoRecords.stream().map(e -> e.groupName).collect(Collectors.toList()));
        }

        // build new group name list
        List<String> newGroupNameList = new ArrayList<>();
        for (String groupName : groupNameList) {
            if (groupNameSet.contains(groupName)) {
                newGroupNameList.add(groupName);
            } else {
                String newGroupName = groupName + "_$" + schemaName.toLowerCase();
                if (groupNameSet.isEmpty() || groupNameSet.contains(newGroupName)) {
                    newGroupNameList.add(newGroupName);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID, String.format(
                        "invalid locality specification for db_pools, The group name [%s] does not exist.",
                        groupName));
                }
            }
        }

        return newGroupNameList;
    }

    public static String unwrapGroupName(String wrappedName, String schemaName) {
        if (schemaName == null || schemaName.isEmpty()) {
            return wrappedName;
        }
        final int len = wrappedName.length();
        final int suffixLen = schemaName.length() + 2;
        if (len > suffixLen && wrappedName.startsWith("_$", len - suffixLen)) {
            return wrappedName.substring(0, len - suffixLen);
        }
        return wrappedName;
    }

    public Map<String, List<String>> getProxyConfig() {
        return proxyConfig;
    }
}
