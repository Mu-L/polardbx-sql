package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

public class SqlDropSecret extends SqlDdl {

    private static final SqlOperator OPERATOR =
        new SqlDropSecretOperator();

    private final String secretName;
    private final boolean ifExists;

    public SqlDropSecret(SqlParserPos pos, String secretName, boolean ifExists) {
        super(OPERATOR, pos);
        this.secretName = secretName;
        this.ifExists = ifExists;
    }

    public String getSecretName() {
        return secretName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.DROP_SECRET;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP SECRET");
        writer.literal(secretName);
    }

    public static class SqlDropSecretOperator extends SqlSpecialOperator {
        public SqlDropSecretOperator() {
            super("DROP SECRET", SqlKind.DROP_SECRET);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            return typeFactory.createStructType(
                ImmutableList.of(new RelDataTypeFieldImpl("RESULT", 0,
                    typeFactory.createSqlType(SqlTypeName.CHAR))));
        }
    }
}
