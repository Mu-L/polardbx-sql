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

package com.alibaba.polardbx.server.handler.privileges.polar;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.gms.privilege.PolarPrivilegeData;
import com.alibaba.polardbx.gms.privilege.PolarReservedAccounts;
import com.alibaba.polardbx.gms.privilege.PolarRolePrivilege;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement.UserSpecification;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.privilege.Host;
import com.alibaba.polardbx.common.privilege.PasswdRuleConfig;
import com.alibaba.polardbx.common.privilege.UserPasswdChecker;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement.UserSpecification;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.server.ServerConnection;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.checkDrdsRoot;
import static com.alibaba.polardbx.server.handler.privileges.polar.PolarHandlerCommon.encryptPassword;
import static com.alibaba.polardbx.server.util.AuditPrivilege.polarAudit;

/**
 * @author shicai.xsc 2020/3/5 20:49
 * @since 5.0.0.0
 */
public class PolarCreateUserHandler extends AbstractPrivilegeCommandHandler {
    private static final Logger logger = LoggerFactory.getLogger(PolarCreateUserHandler.class);

    private final MySqlCreateUserStatement stmt;

    public PolarCreateUserHandler(ByteString sql,
                                  ServerConnection serverConn,
                                  PolarAccountInfo granter,
                                  PolarPrivManager polarPrivManager,
                                  MySqlCreateUserStatement stmt) {
        super(sql, serverConn, granter, polarPrivManager);
        this.stmt = stmt;
    }

    private static void checkGrantees(List<PolarAccountInfo> grantees) {
        checkDrdsRoot(grantees);

        final PasswdRuleConfig passwdRuleConfig =
            CobarServer.getInstance().getConfig().getSystem().getPasswordRuleConfig();
        for (PolarAccountInfo user : grantees) {
            if (!Host.verify(user.getHost())) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_HOST);
            }

            if (!UserPasswdChecker.verifyUsername(user.getUsername())) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_USERNAME);
            }

            String password = user.getPassword() == null ? "" : user.getPassword();
            UserPasswdChecker.verifyPassword(password, passwdRuleConfig);
        }
    }

    private List<PolarAccountInfo> checkAndGetGrantees() {
        List<PolarAccountInfo> grantees = new ArrayList<>();
        for (UserSpecification spec : stmt.getUsers()) {
            AccountType accountType;
            switch (stmt.getAccountType()) {
            case NORMAL:
            default:
                accountType = AccountType.USER;
                break;
            case DBA:
                if (!getGranter().isGod()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPERATION_NOT_ALLOWED,
                        "Can not create DBA user.");
                }
                accountType = AccountType.DBA;
                break;
            }
            MySqlUserName user = null;
            if (spec.getUser() instanceof MySqlUserName) {
                user = (MySqlUserName) spec.getUser();
            } else {
                /*
                 * If host part omitted, defaults to '%'.
                 */
                user = MySqlUserName.fromExpr(spec.getUser());
                user.setHost("%");
                user.setIdentifiedBy(((SQLCharExpr)spec.getPassword()).getText());
            }
            PolarAccountInfo userInfo = new PolarAccountInfo(PolarAccount.newBuilder()
                .setAccountType(accountType)
                .setUsername(user.getUserName())
                .setHost(user.getHost())
                .setPassword(spec.getIdentifiedBy())
                .build());
            grantees.add(userInfo);
        }

        checkGrantees(grantees);

        return grantees;
    }

    @Override
    protected void doHandle() {
        List<PolarAccountInfo> grantees = checkAndGetGrantees();
        encryptPassword(grantees);

        PolarAccountInfo granter = getGranter();
        PolarPrivManager.getInstance().createAccount(granter, getServerConn().getActiveRoles(),
            grantees, stmt.isIfNotExists());
        polarAudit(getServerConn(), getMaskSql(), AuditAction.CREATE_USER);
    }

    private String getMaskSql() {
        return stmt.toString(VisitorFeature.OutputMaskPassword);
    }

    @Override
    protected SqlKind getSqlKind() {
        return SqlKind.CREATE_USER;
    }
}
