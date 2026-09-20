package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;

public class DrdsDropRoutingRuleStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private boolean ifExist;

    private List<SQLName> ruleNames;

    public DrdsDropRoutingRuleStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (this.ruleNames != null) {
                for (SQLName sqlName : ruleNames) {
                    sqlName.accept(visitor);
                }
            }
        }
        visitor.endVisit(this);
    }

    @Override
    public DrdsDropRoutingRuleStatement clone() {
        DrdsDropRoutingRuleStatement x = new DrdsDropRoutingRuleStatement();
        x.ifExist = this.ifExist;
        if (this.ruleNames != null) {
            List<SQLName> xRuleNames = new ArrayList<SQLName>(this.ruleNames.size());
            for (SQLName sqlName : ruleNames) {
                xRuleNames.add(sqlName.clone());
            }
            x.setRuleNames(xRuleNames);
        }
        return x;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DrdsDropRoutingRuleStatement that = (DrdsDropRoutingRuleStatement) o;
        return Objects.equal(this.ifExist, that.ifExist) && Objects.equal(this.ruleNames, that.ruleNames);
    }

    @Override
    public int hashCode() {
        int result = Boolean.valueOf(ifExist).hashCode();
        result = 31 * result + (ruleNames != null ? ruleNames.hashCode() : 0);
        return result;
    }

    public boolean isIfExist() {
        return ifExist;
    }

    public void setIfExist(boolean ifExist) {
        this.ifExist = ifExist;
    }

    public List<SQLName> getRuleNames() {
        return ruleNames;
    }

    public void setRuleNames(List<SQLName> ruleNames) {
        this.ruleNames = ruleNames;
    }

}
