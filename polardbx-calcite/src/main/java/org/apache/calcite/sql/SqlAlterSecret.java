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
import java.util.Map;

public class SqlAlterSecret extends SqlDdl {

    private static final SqlOperator OPERATOR =
        new SqlAlterSecretOperator();

    private final String secretName;
    private final Map<String, String> properties;

    public SqlAlterSecret(SqlParserPos pos, String secretName, Map<String, String> properties) {
        super(OPERATOR, pos);
        this.secretName = secretName;
        this.properties = properties;
    }

    public String getSecretName() {
        return secretName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.ALTER_SECRET;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER SECRET");
        writer.literal(secretName);
    }

    public static class SqlAlterSecretOperator extends SqlSpecialOperator {
        public SqlAlterSecretOperator() {
            super("ALTER SECRET", SqlKind.ALTER_SECRET);
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
