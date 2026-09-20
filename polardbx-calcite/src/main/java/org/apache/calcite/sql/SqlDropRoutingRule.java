package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

public class SqlDropRoutingRule extends SqlDal {
    private static final SqlOperator OPERATOR = new SqlDropRoutingRuleOperator();

    private List<SqlIdentifier> ruleNames;

    private boolean ifExists;

    public SqlDropRoutingRule(SqlParserPos pos, List<SqlIdentifier> ruleNames, boolean ifExists) {
        super(pos);
        this.ruleNames = ruleNames;
        this.ifExists = ifExists;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.DROP_ROUTING_RULE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("DROP");
        writer.sep("ROUTING_RULE");
        if (ifExists) {
            writer.sep("IF EXISTS");
        }
        if (ruleNames != null) {
            if (!ruleNames.isEmpty()) {
                writer.print(ruleNames.get(0).getSimple());
            }
            for (int i = 1; i < ruleNames.size(); ++i) {
                writer.print(", ");
                writer.print(ruleNames.get(i).getSimple());
            }
        }
        writer.endList(selectFrame);
    }

    public static class SqlDropRoutingRuleOperator extends SqlSpecialOperator {

        public SqlDropRoutingRuleOperator() {
            super("DROP_ROUTING_RULE", SqlKind.DROP_ROUTING_RULE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Dropped_Routing_Rule_Count", 0,
                typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)));
            return typeFactory.createStructType(columns);
        }
    }

    public List<SqlIdentifier> getRuleNames() {
        return ruleNames;
    }

    public void setRuleNames(List<SqlIdentifier> ruleNames) {
        this.ruleNames = ruleNames;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

}
