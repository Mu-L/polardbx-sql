package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Pair;

public class SqlCreateRoutingRule extends SqlDal {
    private static final SqlSpecialOperator OPERATOR = new SqlCreateRoutingRuleOperator();

    private boolean ifNotExists;

    private SqlIdentifier ruleName;

    private SqlCharStringLiteral userName;

    private SqlCharStringLiteral templateId;

    private SqlNodeList keywords;

    private Pair<SqlNode, SqlCharStringLiteral> with;

    public SqlCreateRoutingRule(SqlParserPos pos, boolean ifNotExists, SqlIdentifier ruleName,
                                SqlCharStringLiteral userName,
                                SqlCharStringLiteral templateId, SqlNodeList keywords,
                                Pair<SqlNode, SqlCharStringLiteral> with) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.ruleName = ruleName;
        this.userName = userName;
        this.templateId = templateId;
        this.keywords = keywords;
        this.with = with;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CREATE_ROUTING_RULE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("CREATE ROUTING_RULE");
        if (ifNotExists) {
            writer.sep("IF NOT EXISTS");
        }
        ruleName.unparse(writer, leftPrec, rightPrec);

        writer.keyword("TO");
        userName.unparse(writer, leftPrec, rightPrec);

        if (templateId != null) {
            writer.print("FILTER BY TEMPLATE(");
            templateId.unparse(writer, leftPrec, rightPrec);
            writer.keyword(")");
        }

        if (keywords != null) {
            writer.print("FILTER BY KEYWORD(");
            keywords.unparse(writer, leftPrec, rightPrec);
            writer.keyword(")");
        }

        writer.sep("WITH");

        with.getKey().unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        with.getValue().unparse(writer, leftPrec, rightPrec);

        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public static class SqlCreateRoutingRuleOperator extends SqlSpecialOperator {
        public SqlCreateRoutingRuleOperator() {
            super("CREATE_ROUTING_RULE", SqlKind.CREATE_ROUTING_RULE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory
                .createStructType(
                    ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("Create_Routing_Rule_Result",
                        0,
                        columnType)));
        }
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public SqlIdentifier getRuleName() {
        return ruleName;
    }

    public void setRuleName(SqlIdentifier ruleName) {
        this.ruleName = ruleName;
    }

    public SqlCharStringLiteral getUserName() {
        return userName;
    }

    public void setUserName(SqlCharStringLiteral userName) {
        this.userName = userName;
    }

    public SqlCharStringLiteral getTemplateId() {
        return templateId;
    }

    public void setTemplateId(SqlCharStringLiteral templateId) {
        this.templateId = templateId;
    }

    public SqlNodeList getKeywords() {
        return keywords;
    }

    public void setKeywords(SqlNodeList keywords) {
        this.keywords = keywords;
    }

    public Pair<SqlNode, SqlCharStringLiteral> getWith() {
        return with;
    }

    public void setWith(
        Pair<SqlNode, SqlCharStringLiteral> with) {
        this.with = with;
    }
}
