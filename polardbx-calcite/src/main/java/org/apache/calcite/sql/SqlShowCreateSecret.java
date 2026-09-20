package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

public class SqlShowCreateSecret extends SqlShow {
    private static final SqlSpecialOperator OPERATOR = new SqlShowCreateSecretOperator();
    private final String secretName;

    public SqlShowCreateSecret(SqlParserPos pos, String secretName) {
        super(pos,
            ImmutableList.of(SqlSpecialIdentifier.CREATE, SqlSpecialIdentifier.SECRETS),
            ImmutableList.of(),
            null, null, null, null);
        this.secretName = secretName;
    }

    public String getSecretName() {
        return secretName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_CREATE_SECRET;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW CREATE SECRET");
        new SqlIdentifier(secretName, getParserPosition()).unparse(writer, leftPrec, rightPrec);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static class SqlShowCreateSecretOperator extends SqlSpecialOperator {

        public SqlShowCreateSecretOperator() {
            super("SHOW_CREATE_SECRET", SqlKind.SHOW_CREATE_SECRET);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("SECRET_NAME", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("CREATE_STATEMENT", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }
}
