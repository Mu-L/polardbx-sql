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
public class SqlUdfParamsExpr extends SqlNode {

    protected SqlNode functionName;
    protected SqlNode paramsContent;

    public SqlUdfParamsExpr() {
        super(SqlParserPos.ZERO);
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        SqlUdfParamsExpr newExpr = new SqlUdfParamsExpr();
        if (functionName != null) {
            newExpr.setFunctionName(functionName);
        }
        if (paramsContent != null) {
            newExpr.setParamsContent(paramsContent.clone(pos));
        }
        return newExpr;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("UDF_PARAMS ( \n");
        if (functionName != null) {
            writer.print(functionName.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql());
        }
        writer.print(", \n");
        if (paramsContent != null) {
            writer.print(paramsContent.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql());
        }
        writer.print(" \n)");
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {

    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {

        if (this.functionName != null) {
            this.functionName.accept(visitor);
        }

        if (this.paramsContent != null) {
            this.paramsContent.accept(visitor);
        }

        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {

        if (node == this) {
            return true;
        }

        SqlUdfParamsExpr otherUdfParamsExpr = (SqlUdfParamsExpr) node;
        SqlNode otherParamsType = otherUdfParamsExpr.getFunctionName();
        SqlNode otherParamsContent = otherUdfParamsExpr.getParamsContent();

        if (!equalDeep(this.functionName, otherParamsType, litmus, context)) {
            return false;
        }

        if (!equalDeep(this.paramsContent, otherParamsContent, litmus, context)) {
            return false;
        }

        return true;
    }

    public SqlNode getFunctionName() {
        return functionName;
    }

    public void setFunctionName(SqlNode functionName) {
        this.functionName = functionName;
    }

    public SqlNode getParamsContent() {
        return paramsContent;
    }

    public void setParamsContent(SqlNode paramsContent) {
        this.paramsContent = paramsContent;
    }
}
