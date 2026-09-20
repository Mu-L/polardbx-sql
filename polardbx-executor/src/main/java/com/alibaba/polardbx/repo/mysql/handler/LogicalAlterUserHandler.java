package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterUserStatement;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarLoginErr;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAlterUser;

import java.sql.Statement;
import java.util.Collections;
import java.util.Map;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_SERVER;

/**
 * @author pangzhaoxing
 */
public class LogicalAlterUserHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalAlterUserHandler.class);

    public LogicalAlterUserHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlAlterUser sqlAlterUser = (SqlAlterUser) ((LogicalDal) logicalPlan).getNativeSqlNode();
        if (sqlAlterUser.getLock() != null) {
            return handlerAlterUserLock(sqlAlterUser, executionContext);
        }

        if (sqlAlterUser.getReadStrategy() != null) {
            return handlerAlterUserReadStrategy(sqlAlterUser,
                executionContext);
        }
        return null;
    }

    public Cursor handlerAlterUserLock(SqlAlterUser sqlAlterUser, ExecutionContext executionContext) {
        checkPrivilege(executionContext);

        PolarAccount user = sqlAlterUser.getUser().toPolarAccount();
        PolarAccountInfo userToChange = PolarPrivManager.getInstance()
            .getExactUser(user.getUsername(), user.getHost() == null ? "%" : user.getHost());

        boolean lock = sqlAlterUser.getLock();
        if (userToChange == null) {
            throw new TddlRuntimeException(ERR_SERVER, "User " + user.getIdentifier() + " does not exist.");
        }

        if (userToChange != null && userToChange.getInstPriv().isAccountLocked() != lock) {
            PolarPrivManager.getInstance().runWithMetaDBConnection(conn -> {
                logger.info("Starting to " + (lock ? "lock" : "unlock") + " for user: " + userToChange.getAccount()
                    .getIdentifier());
                try {
                    String sql = PolarPrivUtil.getUpdateUserPrivSql(userToChange,
                        Collections.singletonList(PolarPrivUtil.ACCOUNT_LOCKED),
                        Collections.singletonList(lock ? "1" : "0"));
                    try (Statement stmt = conn.createStatement()) {
                        stmt.executeUpdate(sql);
                    }
                    conn.commit();
                    logger.info("Succeeded to " + (lock ? "lock" : "unlock") + " for " + userToChange.getAccount()
                        .getIdentifier());
                    PolarPrivManager.getInstance()
                        .reloadAccounts(conn, Collections.singletonList(userToChange.getAccount()));
                } catch (Exception e) {
                    logger.error(
                        "Failed to " + (lock ? "lock" : "unlock") + " for " + userToChange.getAccount().getIdentifier(),
                        e);
                    throw new TddlRuntimeException(ERR_SERVER, "Failed to persist account data.", e);
                }
            });
            PolarPrivManager.getInstance().triggerReload();
        }

        if (!lock) {
            //clear login error count
            Map<String, PolarLoginErr> loginErrMap = PolarPrivManager.getInstance().getLoginErrMap();
            for (Map.Entry<String, PolarLoginErr> entry : loginErrMap.entrySet()) {
                String key = entry.getKey();
                PolarAccount loginErrAccount = PolarAccount.fromIdentifier(key);
                if (userToChange.isMatch(loginErrAccount.getUsername(), loginErrAccount.getHost())) {
                    PolarPrivManager.getInstance()
                        .clearLoginErrorCount(loginErrAccount.getUsername(), loginErrAccount.getHost());
                }
            }
        }
        return new AffectRowCursor(0);
    }

    public Cursor handlerAlterUserReadStrategy(SqlAlterUser sqlAlterUser, ExecutionContext executionContext) {
        String sql;
        String ruleName = RoutingRuleManager.INNER_RULE + sqlAlterUser.getUser().getUser();
        String readStrategy = sqlAlterUser.getReadStrategy().getNlsString().getValue();
        if (!ruleName.matches("^[a-zA-Z0-9_$]+$")) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "ruleName: " + ruleName
                + " must be composed solely of letters, digits, underscores, and dollar signs!");
        }
        if (MySqlAlterUserStatement.ReadStrategy.FOLLOWER.name().equalsIgnoreCase(readStrategy)) {
            sql = String.format("create routing_rule '%s' to '%s' with type=%s",
                ruleName, sqlAlterUser.getUser().getUser(), readStrategy);
        } else if (MySqlAlterUserStatement.ReadStrategy.STALE.name().equalsIgnoreCase(readStrategy)) {
            sql = String.format("create routing_rule '%s' to '%s' with type=%s",
                ruleName, sqlAlterUser.getUser().getUser(), readStrategy);
        } else if (MySqlAlterUserStatement.ReadStrategy.NONE.name().equalsIgnoreCase(readStrategy)) {
            sql = String.format("drop routing_rule if exists '%s'", ruleName);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "read strategy " + readStrategy);
        }
        ExecutionContext newExecutionContext = executionContext.copy();
        newExecutionContext.newStatement();
        ExecutionPlan executionPlan = Planner.getInstance().plan(sql, newExecutionContext);
        try {
            return ExecutorHelper.execute(executionPlan.getPlan(), newExecutionContext);
        } catch (Exception e) {
            if (e.getMessage().contains("already exists")) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "read strategy already set");
            }
            throw e;
        }
    }

    private void checkPrivilege(ExecutionContext ec) {
        PolarAccountInfo polarUserInfo = ec.getPrivilegeContext().getPolarUserInfo();
        if (!polarUserInfo.getAccountType().isSuperUser()
            && polarUserInfo.getAccountType() != AccountType.SSO) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED, "alter user", ec.getUser(),
                ec.getClientIp());
        }
    }

}
