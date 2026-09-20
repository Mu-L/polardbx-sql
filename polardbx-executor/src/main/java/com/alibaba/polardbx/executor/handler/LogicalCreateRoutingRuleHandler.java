package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleAccessor;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalRoutingRule;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleManager;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingType;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreateRoutingRule;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class LogicalCreateRoutingRuleHandler extends HandlerCommon {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalCreateRoutingRuleHandler.class);

    private static final int MAX_RULE_NAME = 100;

    private static final int MAX_TEMPLATE = 10;

    private static final int MAX_KEYWORD = 500;

    public LogicalCreateRoutingRuleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlCreateRoutingRule sqlCreateRoutingRule =
            (SqlCreateRoutingRule) ((LogicalRoutingRule) logicalPlan).getSqlDal();
        String ruleName = sqlCreateRoutingRule.getRuleName().getSimple();
        String userName = sqlCreateRoutingRule.getUserName().getNlsString().getValue();

        String templateId = null;
        if (sqlCreateRoutingRule.getTemplateId() != null) {
            templateId = sqlCreateRoutingRule.getTemplateId().getNlsString().getValue();
        }
        SqlNodeList keywordsList = sqlCreateRoutingRule.getKeywords();
        List<String> keywords = Lists.newArrayList();
        if (keywordsList != null) {
            for (int i = 0; i < keywordsList.size(); ++i) {
                String tmpString = ((SqlCharStringLiteral) keywordsList.get(i)).getNlsString().getValue().toLowerCase();
                if (StringUtils.isEmpty(tmpString)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE,
                        String.format("the %d-th keyword is empty!", i));
                }
                keywords.add(tmpString);
            }
        }
        String routingType = sqlCreateRoutingRule.getWith().getValue().getNlsString().getValue();

        // validate routing rule
        validateRoutingRule(ruleName,
            userName,
            templateId,
            keywords,
            routingType,
            executionContext);

        RoutingRuleRecord record = new RoutingRuleRecord();
        record.instId = InstIdUtil.getInstId();
        record.ruleName = ruleName;
        record.userName = userName;
        record.templateId = templateId;
        record.keywords = keywords;
        record.routingType = routingType;

        return commitRoutingRule(record, sqlCreateRoutingRule.isIfNotExists());
    }

    protected Cursor commitRoutingRule(RoutingRuleRecord record, Boolean ifNotExists) {
        String dataId = MetaDbDataIdBuilder.getRoutingRuleDataId(InstIdUtil.getInstId());
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            try {
                metaDbConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                RoutingRuleAccessor routingRuleAccessor = RoutingRuleAccessor.create(metaDbConn);
                MetaDbUtil.beginTransaction(metaDbConn);
                if (!CollectionUtils.isEmpty(routingRuleAccessor.lockRule(record.instId, record.ruleName))) {
                    if (ifNotExists) {
                        return new AffectRowCursor(0);
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE,
                            record.ruleName + " already exists!");
                    }
                }
                routingRuleAccessor.insert(record);
                MetaDbConfigManager.getInstance().notify(dataId, metaDbConn);
                MetaDbUtil.commit(metaDbConn);
            } catch (Throwable t) {
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(t), LOGGER, "remove routing rule");
                throw t;
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }
            MetaDbConfigManager.getInstance().sync(dataId);
            return new AffectRowCursor(1);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    /**
     * Validates the routing rule parameters for correctness and consistency
     *
     * @param ruleName The name of the routing rule
     * @param userName The username associated with the routing rule
     * @param templateId The template ID for the routing rule
     * @param keywords The keywords for the routing rule
     * @param routingType The type of routing for the routing rule
     * @param ec The execution context, containing information such as user privileges
     */
    protected void validateRoutingRule(String ruleName,
                                       String userName,
                                       String templateId,
                                       List<String> keywords,
                                       String routingType,
                                       ExecutionContext ec) {
        validateRuleName(ruleName);
        validatePrivilege(userName, ec);
        validateTemplateIdAndKeyWords(templateId, keywords);
        validateRoutingType(routingType);
        validateUser(userName, templateId, keywords);
        validateFollower(ruleName, userName, templateId, keywords, routingType);
    }

    /**
     * Validates the rule name for the routing rule
     *
     * @param ruleName The name of the routing rule
     * Checks if the rule name is empty or exceeds the maximum length
     */
    protected void validateRuleName(String ruleName) {
        // check string is not empty or too long
        if (StringUtils.isEmpty(ruleName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "ruleName is empty!");
        }
        if (ruleName.length() > MAX_RULE_NAME) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "ruleName too long!");
        }
        if (!ruleName.matches("^[a-zA-Z0-9_$]+$")) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "ruleName: " + ruleName
                + " must be composed solely of letters, digits, underscores, and dollar signs!");
        }
    }

    /**
     * Validates the user's privilege to perform certain operations.
     * <p>
     * This method primarily checks if the user has the necessary permissions based on the user name and the current execution context.
     * It ensures that the user either matches the specified user name or possesses sufficient privileges to perform the operation.
     *
     * @param userName The user name to validate, which is the basis for permission verification.
     * @param ec The execution context, containing information about the current user's privileges and superuser status.
     * @throws TddlRuntimeException If the user name is empty, not the current user, or if attempting to assign '%', which is only allowed for superusers.
     */
    protected void validatePrivilege(String userName, ExecutionContext ec) {
        // Check if the user name is empty, if so, throw an exception
        if (StringUtils.isEmpty(userName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "user name is empty!");
        }

        // Determine if the user name is a wildcard representing all users
        boolean allUser = RoutingRuleManager.ALL_USER.equals(userName);
        if (allUser) {
            // If it's a wildcard for all users, only allow superusers to proceed
            if (ec.isSuperUser()) {
                return;
            }
            // If not a superuser, throw an exception
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "only super user can assign user '%'!");
        }

        // If it's not a wildcard and the current user is a superuser, no further checks are needed
        if (ec.isSuperUser()) {
            if (PolarPrivUtil.POLAR_ROOT.equalsIgnoreCase(userName)) {
                if (!ec.isGod()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "only god can assign polar_root!");
                }
            }
            return;
        }

        // If the current user is not a superuser, verify that the user name matches the current user
        if (!ec.getPrivilegeContext()
            .getUser()
            .equals(userName)) {
            // If it doesn't match, throw an exception
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "user name is not current user!");
        }
    }

    /**
     * Validates the template ID and keywords for the routing rule
     *
     * @param templateId The template ID for the routing rule
     * @param keywords The keywords for the routing rule
     * Ensures that the template ID and keywords are not set at the same time, and checks their lengths
     */
    protected void validateTemplateIdAndKeyWords(String templateId, List<String> keywords) {
        if ((!StringUtils.isEmpty(templateId))
            && (!CollectionUtils.isEmpty(keywords))) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE,
                "templateId and keyword can't be set at the same time!");
        }
        if (!StringUtils.isEmpty(templateId)) {
            if (templateId.length() > MAX_TEMPLATE) {
                throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "templateId too long!");
            }
        }
        if (!CollectionUtils.isEmpty(keywords)) {
            if (keywords.stream().mapToLong(String::length).sum() > MAX_KEYWORD) {
                throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "keyword too long!");
            }
        }
    }

    /**
     * Validates the routing type for the routing rule
     *
     * @param routingType The type of routing for the routing rule
     * Checks if the routing type is valid
     */
    protected void validateRoutingType(String routingType) {
        if (!RoutingType.isRoutingType(routingType)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE,
                "routingType " + routingType + " is not valid!");
        }
    }

    protected void validateUser(String userName,
                                String templateId,
                                List<String> keyWords) {
        if (RoutingRuleManager.ALL_USER.equals(userName)) {
            if (StringUtils.isEmpty(templateId) &&
                CollectionUtils.isEmpty(keyWords)) {
                throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE,
                    "can't be set for all user without templateId or keyWords");
            }
            return;
        }
        if (PolarPrivManager.getInstance()
            .getAccountPrivilegeData().
            getAccountInfo()
            .stream()
            .noneMatch(x -> x.getUsername().equalsIgnoreCase(userName))) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "unknown user " + userName);
        }
    }

    /**
     * Validates the follower routing rule parameters for correctness and consistency
     *
     * @param ruleName The name of the routing rule
     * @param userName The username associated with the routing rule
     * @param templateId The template ID for the routing rule
     * @param keywords The keywords for the routing rule
     * @param routingType The type of routing for the routing rule
     * @throws TddlRuntimeException If the validation fails
     */
    protected void validateFollower(String ruleName,
                                    String userName,
                                    String templateId,
                                    List<String> keywords,
                                    String routingType) {
        // 首先检查路由类型是否为FOLLOWER，如果不是则直接返回，无需进一步验证
        if (!RoutingType.isFollower(RoutingType.getType(routingType))) {
            return;
        }

        if (!ConfigDataMode.isMasterMode()) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "follower can't be set for non-master instance");
        }

        // 检查用户名是否为所有用户标识，如果是则抛出异常，因为follower路由规则不能为所有用户设置
        if (RoutingRuleManager.ALL_USER.equals(userName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "follower can't be set for all user");
        }

        if (!StringUtils.isEmpty(templateId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "templateId for follower can't be set!");
        }
        if (!CollectionUtils.isEmpty(keywords)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE, "keyword for follower can't be set!");
        }
        // 检查rule name是否以INNER_RULE开头（忽略大小写）
        if (!ruleName.toLowerCase().startsWith(RoutingRuleManager.INNER_RULE)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE,
                "rule name " + ruleName + " for follower is invalid");
        }

        // 验证ruleName的剩余部分是否与userName匹配（忽略大小写）
        String ruleNameSuffix = ruleName.substring(RoutingRuleManager.INNER_RULE.length());
        if (!ruleNameSuffix.equalsIgnoreCase(userName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTING_RULE,
                "follower name must be " + RoutingRuleManager.INNER_RULE + " + userName");
        }
    }
}