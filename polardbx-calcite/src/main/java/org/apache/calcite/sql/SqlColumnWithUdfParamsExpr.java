package org.apache.calcite.sql;

import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

/**
 * @author chenghui.lch
 */
public class SqlColumnWithUdfParamsExpr extends SqlNode {

    protected SqlNode columnName;
    protected SqlNode udfParams;

    public SqlColumnWithUdfParamsExpr() {
        super(SqlParserPos.ZERO);
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        SqlColumnWithUdfParamsExpr newExpr = new SqlColumnWithUdfParamsExpr();
        if (columnName != null) {
            newExpr.setColumnName(columnName.clone(pos));
        }
        if (udfParams != null) {
            newExpr.setUdfParams(udfParams.clone(pos));
        }
        return newExpr;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (columnName != null) {
            writer.print(columnName.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql());
        }
        if (udfParams != null) {
            writer.print(" WITH ");
            writer.print(udfParams.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql());
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {

    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {

        if (this.columnName != null) {
            this.columnName.accept(visitor);
        }

        if (this.udfParams != null) {
            this.udfParams.accept(visitor);
        }

        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        if (node == this) {
            return true;
        }

        SqlColumnWithUdfParamsExpr otherColumnWithUdfParamsExpr = (SqlColumnWithUdfParamsExpr) node;
        SqlNode colName = otherColumnWithUdfParamsExpr.getColumnName();
        SqlNode udfParams = otherColumnWithUdfParamsExpr.getUdfParams();

        if (!equalDeep(this.columnName, colName, litmus, context)) {
            return false;
        }

        if (!equalDeep(this.udfParams, udfParams, litmus, context)) {
            return false;
        }

        return true;
    }

    public SqlNode getColumnName() {
        return columnName;
    }

    public void setColumnName(SqlNode columnName) {
        this.columnName = columnName;
    }

    public SqlNode getUdfParams() {
        return udfParams;
    }

    public void setUdfParams(SqlNode udfParams) {
        this.udfParams = udfParams;
    }
}
