/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.privilege.PolarAccount;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author pangzhaoxing
 * <p>
 * 所有的加密规则的搜索，需要收敛到match和fastMatch两个方法
 */
public class EncdbRuleMatchTree {

    private static final Logger LOG = LoggerFactory.getLogger(EncdbRuleMatchTree.class);

    public static final String ALL_DB = "*";

    public static final String ALL_TB = "*";

    public static final String ALL_COL = "*";

    //db -> table -> col -> rule_names
    private Map<String, Map<String, Map<String, EncdbRule>>> tree;

    public EncdbRuleMatchTree() {
        tree = new HashMap<>();
    }

    /**
     * @param originColumnNamesList origin column names derived from sql resultSet columns
     * @return whether the output columns need encryption
     */
    public boolean[] getColumnEncBitmap(List<List<String[]>> originColumnNamesList, PolarAccount account, boolean forceUserMatch) {
        return getColumnRuleMatchBitmap(originColumnNamesList, account, EncdbRule.EncdbRuleType.ENCRYPTION, forceUserMatch);
    }

    /**
     * @param originColumnNamesList origin column names derived from sql resultSet columns
     * @return whether the output columns need masking
     */
    public boolean[] getColumnMaskBitmap(List<List<String[]>> originColumnNamesList, PolarAccount account, boolean forceUserMatch) {
        return getColumnRuleMatchBitmap(originColumnNamesList, account, EncdbRule.EncdbRuleType.MASKING, forceUserMatch);
    }

    private boolean[] getColumnRuleMatchBitmap(List<List<String[]>> originColumnNamesList, PolarAccount account,
                                               EncdbRule.EncdbRuleType ruleType, boolean forceUserMatch) {
        boolean[] columnMatchBitmap = new boolean[originColumnNamesList.size()];
        boolean hasMatchColumn = false;
        for (int i = 0; i < originColumnNamesList.size(); i++) {
            if (getColumnMatchBit(originColumnNamesList.get(i), account, ruleType, forceUserMatch)) {
                columnMatchBitmap[i] = true;
                hasMatchColumn = true;
            }
        }
        return hasMatchColumn ? columnMatchBitmap : null;
    }

    private boolean getColumnMatchBit(List<String[]> originColumnNames, PolarAccount account,
                                      EncdbRule.EncdbRuleType ruleType) {
        return getColumnMatchBit(originColumnNames, account, ruleType, false);
    }

    private boolean getColumnMatchBit(List<String[]> originColumnNames, PolarAccount account,
                                      EncdbRule.EncdbRuleType ruleType, boolean forceUserMatch) {
        for (String[] col : originColumnNames) {
            EncdbRule rule = matchRule(col[0], col[1], col[2]);
            if (rule != null && rule.getRuleType() == ruleType && rule.isEnable()) {
                if (forceUserMatch) {
                    return true;
                }
                Boolean isRestrictedAccess = rule.isRestrictedAccess(account);
                if (isRestrictedAccess != null && isRestrictedAccess) {
                    return true;
                }
            }
        }
        return false;
    }


    public List<Set<String>> getColumnMatchRulesList(List<List<String[]>> originColumnNamesList) {
        List<Set<String>> matchRulesList = new ArrayList<>(originColumnNamesList.size());
        boolean hasMatchRule = false;
        for (int i = 0; i < originColumnNamesList.size(); i++) {
            matchRulesList.add(getColumnMatchRules(originColumnNamesList.get(i)));
            if (matchRulesList.get(i).size() > 0) {
                hasMatchRule = true;
            }
        }
        return hasMatchRule ? matchRulesList : null;
    }

    private Set<String> getColumnMatchRules(List<String[]> originColumnNames) {
        Set<String> matchRules = new HashSet<>();
        for (String[] col : originColumnNames) {
            String matchRule = match(col[0], col[1], col[2]);
            if (matchRule != null) {
                matchRules.add(matchRule);
            }
        }
        return matchRules;
    }

    /**
     * 获取只包含db、tb的扩散的加密规则
     * 用于规则扩散，因为扩散的规则是只包含一个db和一个tb的
     */
    public Set<String> getSpreadRules(String db, String tb) {
        db = db.toLowerCase();
        tb = tb.toLowerCase();
        if (tree.containsKey(db)) {
            if (tree.get(db).containsKey(tb)) {
                Collection<EncdbRule> matchRules = tree.get(db).get(tb).values();
                Set<String> specificTableRules = matchRules.stream().map(EncdbRule::getName)
                    .filter(ruleName -> ruleName.startsWith(EncdbRuleManager.ENCDB_SPREADED_RULE_PREFIX))
                    .collect(Collectors.toSet());
                return specificTableRules;
            }
        }
        return Collections.emptySet();
    }

    /**
     * @param tableSet a set of <schema, table> pair, the schema may be null
     * @param schema schema of the table whose schema is null in tableSet
     * @return exist any rules match any tables in tableSet
     */
    public boolean fastMatch(Set<Pair<String, String>> tableSet, String schema) {
        for (Pair<String, String> table : tableSet) {
            if (fastMatch(table.getKey() == null ? schema : table.getKey(), table.getValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 找到<db,tb.col>对应的加密规则
     */
    public String match(String db, String tb, String col) {
        EncdbRule rule = matchRule(db, tb, col);
        return rule == null ? null : rule.getName();
    }

    public EncdbRule matchRule(String db, String tb, String col) {
        db = db.toLowerCase();
        tb = tb.toLowerCase();
        col = col.toLowerCase();

        // db -> tb -> col -> rule
        for (Map<String, Map<String, EncdbRule>> dbSubTree : new Map[] {tree.get(db), tree.get(ALL_DB)}) {
            if (dbSubTree == null) {
                continue;
            }
            //tb -> col -> rule
            for (Map<String, EncdbRule> tbSubTree : new Map[] {dbSubTree.get(tb), dbSubTree.get(ALL_DB)}) {
                if (tbSubTree == null) {
                    continue;
                }
                //col -> rule
                for (EncdbRule rule : new EncdbRule[] {tbSubTree.get(col), tbSubTree.get(ALL_COL)}) {
                    if (rule == null) {
                        continue;
                    }
                    return rule;
                }
            }
        }
        return null;
    }

    /**
     * 快速判断<db,tb>行是否有加密规则
     */
    public boolean fastMatch(String db, String tb) {
        db = db.toLowerCase();
        tb = tb.toLowerCase();
        if (tree.containsKey(db) || tree.containsKey(ALL_DB)) {
            // db -> tb -> col -> rule
            for (Map<String, Map<String, EncdbRule>> dbSubTree : new Map[] {tree.get(db), tree.get(ALL_DB)}) {
                if (dbSubTree == null) {
                    continue;
                }
                if (dbSubTree.containsKey(tb) || dbSubTree.containsKey(ALL_TB)) {
                    //tb -> col -> rule_name
                    return true;
                }
            }
        }

        return false;
    }

    public synchronized void insertRule(EncdbRule rule) {
        for (String db : rule.getDbs()) {
            tree.putIfAbsent(db, new HashMap<>());
            Map<String, Map<String, EncdbRule>> dbSubTree = tree.get(db);
            for (String tb : rule.getTbs()) {
                dbSubTree.putIfAbsent(tb, new HashMap<>());
                Map<String, EncdbRule> tbSubTree = dbSubTree.get(tb);
                for (String col : rule.getCols()) {
                    if (tbSubTree.containsKey(col)) {
                        LOG.warn("column should only exist in one encdb rule : "
                                + col + " in " + tbSubTree.get(col) + " 、 " + rule.getName());
                    }
                    tbSubTree.put(col, rule);
                }
            }
        }
    }

}
