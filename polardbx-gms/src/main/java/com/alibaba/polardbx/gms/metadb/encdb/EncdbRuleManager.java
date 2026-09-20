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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author pangzhaoxing
 */
public class EncdbRuleManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(EncdbRuleManager.class);

    public static final String ENCDB_SPREADED_RULE_PREFIX = "_encdb_spreaded_rule_";

    private static final EncdbRuleManager INSTANCE = new EncdbRuleManager();

    private Map<String, EncdbRule> allRules;
    private EncdbRuleMatchTree ruleMatchTree;
    private EncdbUserPrivilegeManager userPrivilegeManager;

    public static EncdbRuleManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        reloadEncRules();
        setupConfigListener();
    }

    private void setupConfigListener() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            EncdbRuleConfigListener listener = new EncdbRuleConfigListener();
            MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, conn);
            MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, listener);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "setup encdb rule config_listener failed");
        }
    }

    private void reloadEncRules() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            EncdbRuleAccessor accessor = new EncdbRuleAccessor();
            accessor.setConnection(conn);
            Map<String, EncdbRule> newAllRules = new HashMap<>();
            EncdbRuleMatchTree newRuleMatchTree = new EncdbRuleMatchTree();
            EncdbUserPrivilegeManager newUserPrivilegeManager = new EncdbUserPrivilegeManager();
            for (EncdbRule rule : accessor.queryAllRules()) {
                //将user privilege rule单独处理
                if (EncdbUserPrivilegeManager.isUserPrivilegeRule(rule)) {
                    newUserPrivilegeManager.loadUserPrivilegeRule(rule);
                    continue;
                }
                newAllRules.put(rule.getName(), rule);
                //只需要将enabled的rule插入EncdbRuleMatchTree
                if (rule.isEnable()) {
                    newRuleMatchTree.insertRule(rule);
                }
            }
            this.allRules = newAllRules;
            this.ruleMatchTree = newRuleMatchTree;
            this.userPrivilegeManager = newUserPrivilegeManager;
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public static int insertEncRules(List<EncdbRule> rules) {
        int affectRows = 0;
        if (rules == null || rules.isEmpty()) {
            return affectRows;
        }
        try (Connection conn = MetaDbUtil.getConnection()) {
            try {
                EncdbRuleAccessor accessor = new EncdbRuleAccessor();
                accessor.setConnection(conn);
                conn.setAutoCommit(false);
                for (EncdbRule rule : rules) {
                    affectRows += accessor.insertRule(rule);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, conn);
                conn.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID);
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
        return affectRows;
    }

    public static int replaceEncRules(List<EncdbRule> rules) {
        int affectRows = 0;
        if (rules == null || rules.isEmpty()) {
            return affectRows;
        }
        try (Connection conn = MetaDbUtil.getConnection()) {
            try {
                EncdbRuleAccessor accessor = new EncdbRuleAccessor();
                accessor.setConnection(conn);
                conn.setAutoCommit(false);
                for (EncdbRule rule : rules) {
                    affectRows += accessor.replaceRule(rule);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, conn);
                conn.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID);
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
        return affectRows;
    }

    public static int deleteEncRule(String ruleName) {
        try (Connection conn = MetaDbUtil.getConnection()) {
            EncdbRuleAccessor accessor = new EncdbRuleAccessor();
            accessor.setConnection(conn);
            int update = accessor.deleteRuleByName(ruleName);
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, conn);
            // wait for all cn to load metadb
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID);
            return update;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static int deleteEncRules(Collection<String> ruleNames) {
        int affectRows = 0;
        if (ruleNames == null || ruleNames.isEmpty()) {
            return affectRows;
        }
        try (Connection conn = MetaDbUtil.getConnection()) {
            try {
                EncdbRuleAccessor accessor = new EncdbRuleAccessor();
                accessor.setConnection(conn);
                conn.setAutoCommit(false);
                for (String ruleName : ruleNames) {
                    affectRows += accessor.deleteRuleByName(ruleName);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, conn);
                conn.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID);
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
        return affectRows;
    }

    public EncdbRule getEncRule(String ruleName) {
        return allRules.get(ruleName);
    }

    public List<EncdbRule> getAllEncRule(){
        return new ArrayList<>(allRules.values());
    }

    public EncdbRuleMatchTree getRuleMatchTree() {
        return ruleMatchTree;
    }

    public EncdbUserPrivilegeManager getUserPrivilegeManager() {
        return userPrivilegeManager;
    }

    protected static class EncdbRuleConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            EncdbRuleManager.getInstance().reloadEncRules();
        }
    }

}
