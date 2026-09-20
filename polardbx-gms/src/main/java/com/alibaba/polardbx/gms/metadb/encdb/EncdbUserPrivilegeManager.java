package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.gms.privilege.PolarAccount;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleMatchTree.*;

/**
 * @author pangzhaoxing
 */
public class EncdbUserPrivilegeManager {

    private static final String USER_PRIV_RULE_NAME = "__POLARX_ENCDB_USER_PRIV_RULE__";

    Set<PolarAccount> fulLAccessUsers = new HashSet<>();

    Set<PolarAccount> restrictedAccessUsers = new HashSet<>();

    public EncdbUserPrivilegeManager() {

    }

    public Set<PolarAccount> getFulLAccessUsers() {
        return fulLAccessUsers;
    }

    public Set<PolarAccount> getRestrictedAccessUsers() {
        return restrictedAccessUsers;
    }

    public static boolean isUserPrivilegeRule(EncdbRule rule) {
        return USER_PRIV_RULE_NAME.equalsIgnoreCase(rule.getName());
    }

    public void loadUserPrivilegeRule(EncdbRule rule) {
        if (!isUserPrivilegeRule(rule)) {
            return;
        }
        fulLAccessUsers.addAll(rule.getFullAccessUsers());
        restrictedAccessUsers.addAll(rule.getRestrictedAccessUsers());
    }

    public static EncdbRule createUserPrivRule(Set<PolarAccount> fulLAccessUsers,
                                               Set<PolarAccount> restrictedAccessUsers) {
        EncdbRule rule = new EncdbRule(USER_PRIV_RULE_NAME, true, fulLAccessUsers, restrictedAccessUsers,
            Collections.singleton(ALL_DB), Collections.singleton(ALL_TB), Collections.singleton(ALL_COL),
            USER_PRIV_RULE_NAME,
            EncdbRule.EncdbRuleType.ENCRYPTION, null);
        return rule;
    }

    public void grantUserPrivilege(PolarAccount account, String privilegeKind) {
        Set<PolarAccount> newRestrictedAccessUsers = new HashSet<>(restrictedAccessUsers);
        Set<PolarAccount> newFullAccessUsers = new HashSet<>(fulLAccessUsers);
        newRestrictedAccessUsers.remove(account);
        newFullAccessUsers.remove(account);
        if (MsgKeyConstants.FULL_ACCESS.equalsIgnoreCase(privilegeKind)) {
            newFullAccessUsers.add(account);
        } else if (MsgKeyConstants.RESTRICTED_ACCESS.equalsIgnoreCase(privilegeKind)) {
            newRestrictedAccessUsers.add(account);
        }
        EncdbRule rule = createUserPrivRule(newFullAccessUsers, newRestrictedAccessUsers);
        EncdbRuleManager.replaceEncRules(Collections.singletonList(rule));
    }

    public boolean isFullAccess(PolarAccount account) {
        if (fulLAccessUsers.contains(account)) {
            return true;
        }
        for (PolarAccount fullAccessUser : fulLAccessUsers) {
            if (fullAccessUser.matches(account.getUsername(), account.getHost())) {
                return true;
            }
        }
        return false;
    }

    public boolean isRestrictedAccess(PolarAccount account) {
        if (restrictedAccessUsers.contains(account)) {
            return true;
        }
        for (PolarAccount restrictedAccessUser : restrictedAccessUsers) {
            if (restrictedAccessUser.matches(account.getUsername(), account.getHost())) {
                return true;
            }
        }
        return false;
    }

}
