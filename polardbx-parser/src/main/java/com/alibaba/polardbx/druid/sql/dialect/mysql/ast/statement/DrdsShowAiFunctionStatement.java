package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * SHOW AI FUNCTION [FROM function_name]
 *
 * <p>Shows model type information for all AI functions or a specific one.
 */
public class DrdsShowAiFunctionStatement extends MySqlStatementImpl implements MySqlShowStatement {

    private String functionName;

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}
