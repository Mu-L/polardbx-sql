package com.alibaba.polardbx.druid.sql.ast.expr;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExprImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author chenghui.lch
 */
public class SQLUdfParamsExpr extends SQLExprImpl {

    protected SQLExpr functionName;
    protected SQLExpr paramsContent;

    public SQLUdfParamsExpr() {
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

        SQLUdfParamsExpr otherTtlExpr = (SQLUdfParamsExpr) obj;
        SQLExpr typeOfOthers = otherTtlExpr.getFunctionName();
        SQLExpr paramsOfOthers = otherTtlExpr.getParamsContent();

        if (functionName != null) {
            if (!functionName.equals(typeOfOthers)) {
                return false;
            }
        } else {
            if (typeOfOthers != null) {
                return false;
            }
        }

        if (paramsContent != null) {
            if (!paramsContent.equals(paramsOfOthers)) {
                return false;
            }
        } else {
            if (paramsOfOthers != null) {
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
        int result = functionName != null ? functionName.hashCode() : 0;
        result = 31 * result + (paramsContent != null ? paramsContent.hashCode() : 0);
        return result;
    }

    @Override
    public SQLExpr clone() {
        SQLUdfParamsExpr newUdfParamsExpr = new SQLUdfParamsExpr();
        if (functionName != null) {
            newUdfParamsExpr.setFunctionName(functionName.clone());
        }

        if (paramsContent != null) {
            newUdfParamsExpr.setParamsContent(paramsContent.clone());
        }
        return newUdfParamsExpr;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (functionName != null) {
                acceptChild(visitor, functionName);
            }

            if (paramsContent != null) {
                acceptChild(visitor, paramsContent);
            }

        }
        visitor.endVisit(this);
    }

    public SQLExpr getFunctionName() {
        return functionName;
    }

    public void setFunctionName(SQLExpr functionName) {
        this.functionName = functionName;
    }

    public SQLExpr getParamsContent() {
        return paramsContent;
    }

    public void setParamsContent(SQLExpr paramsContent) {
        this.paramsContent = paramsContent;
    }
}
