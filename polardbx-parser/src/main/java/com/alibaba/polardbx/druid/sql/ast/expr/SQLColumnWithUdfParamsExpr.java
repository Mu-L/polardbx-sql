package com.alibaba.polardbx.druid.sql.ast.expr;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExprImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author chenghui.lch
 */
public class SQLColumnWithUdfParamsExpr extends SQLExprImpl {

    protected SQLExpr columnName;
    protected SQLExpr sqlUdfParams;

    public SQLColumnWithUdfParamsExpr() {
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        SQLColumnWithUdfParamsExpr otherTtlExpr = (SQLColumnWithUdfParamsExpr) obj;
        SQLExpr otherColumnName = otherTtlExpr.getColumnName();
        SQLExpr otherUdfParams = otherTtlExpr.getSqlUdfParams();

        if (columnName != null) {
            if (!columnName.equals(otherColumnName)) {
                return false;
            }
        } else {
            if (otherColumnName != null) {
                return false;
            }
        }

        if (sqlUdfParams != null) {
            if (!sqlUdfParams.equals(otherUdfParams)) {
                return false;
            }
        } else {
            if (otherUdfParams != null) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        SQLASTVisitor visitor = new MySqlOutputVisitor(sb);
        this.accept(visitor);
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = columnName != null ? columnName.hashCode() : 0;
        result = 31 * result + (sqlUdfParams != null ? sqlUdfParams.hashCode() : 0);
        return result;
    }

    @Override
    public SQLExpr clone() {
        SQLColumnWithUdfParamsExpr newTtlExpr = new SQLColumnWithUdfParamsExpr();
        if (columnName != null) {
            newTtlExpr.setColumnName(columnName.clone());
        }

        if (sqlUdfParams != null) {
            newTtlExpr.setSqlUdfParams(sqlUdfParams.clone());
        }
        return newTtlExpr;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (columnName != null) {
                acceptChild(visitor, columnName);
            }

            if (sqlUdfParams != null) {
                acceptChild(visitor, sqlUdfParams);
            }

        }
        visitor.endVisit(this);
    }

    public SQLExpr getColumnName() {
        return columnName;
    }

    public void setColumnName(SQLExpr columnName) {
        this.columnName = columnName;
    }

    public SQLExpr getSqlUdfParams() {
        return sqlUdfParams;
    }

    public void setSqlUdfParams(SQLExpr sqlUdfParams) {
        this.sqlUdfParams = sqlUdfParams;
    }
}
