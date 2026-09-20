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

public class SqlAlterUser extends SqlDal {

    private SqlUserName user;

    private Boolean lock;

    private boolean ifExists;

    private SqlCharStringLiteral readStrategy;

    private static final SqlSpecialOperator OPERATOR = new SqlAlterUserOperator();

    public SqlAlterUser(SqlParserPos pos, SqlUserName user) {
        super(pos);
        this.user = user;
    }

    public Boolean getLock() {
        return lock;
    }

    public void setLock(Boolean lock) {
        this.lock = lock;
    }

    public SqlCharStringLiteral getReadStrategy() {
        return readStrategy;
    }

    public void setReadStrategy(SqlCharStringLiteral readStrategy) {
        this.readStrategy = readStrategy;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER USER");
        user.unparse(writer, leftPrec, rightPrec);

        if (lock != null) {
            if (lock) {
                writer.keyword("ACCOUNT LOCK");
            } else {
                writer.keyword("ACCOUNT UNLOCK");
            }
        }
        if (readStrategy != null) {
            writer.keyword(" READ_STRATEGY");
            readStrategy.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlUserName getUser() {
        return user;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.ALTER_USER;
    }

    public static class SqlAlterUserOperator extends SqlSpecialOperator {
        public SqlAlterUserOperator() {
            super("ALTER USER", SqlKind.ALTER_USER);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.CHAR)));
            return typeFactory.createStructType(columns);
        }
    }

}
