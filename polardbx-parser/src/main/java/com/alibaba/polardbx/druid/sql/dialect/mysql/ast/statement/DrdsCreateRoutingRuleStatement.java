package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class DrdsCreateRoutingRuleStatement extends MySqlStatementImpl implements SQLCreateStatement {
    private boolean ifNotExists;

    private SQLName ruleName;

    private SQLName userName;

    private SQLCharExpr templateId;

    private SQLListExpr keywords;

    private SQLAssignItem with;

    public DrdsCreateRoutingRuleStatement() {
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (this.ruleName != null) {
                this.ruleName.accept(visitor);
            }
            if (this.userName != null) {
                this.userName.accept(visitor);
            }
            if (this.templateId != null) {
                this.templateId.accept(visitor);
            }
            if (this.keywords != null) {
                this.keywords.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    @Override
    public DrdsCreateRoutingRuleStatement clone() {
        DrdsCreateRoutingRuleStatement x = new DrdsCreateRoutingRuleStatement();
        x.ifNotExists = this.ifNotExists;
        if (this.ruleName != null) {
            x.ruleName = this.ruleName.clone();
            x.ruleName.setParent(x);
        }
        if (this.userName != null) {
            x.userName = this.userName.clone();
            x.userName.setParent(x);
        }
        if (this.templateId != null) {
            x.templateId = this.templateId.clone();
            x.templateId.setParent(x);
        }
        if (this.keywords != null) {
            x.keywords = this.keywords.clone();
            x.keywords.setParent(x);
        }
        if (this.with != null) {
            x.with = this.with.clone();
            x.with.setParent(x);
        }

        return x;
    }

    @Override
    public int hashCode() {
        int result = ifNotExists ? 1 : 0;
        result = 31 * result + (ruleName != null ? ruleName.hashCode() : 0);
        result = 31 * result + (userName != null ? userName.hashCode() : 0);
        result = 31 * result + (templateId != null ? templateId.hashCode() : 0);
        result = 31 * result + (keywords != null ? keywords.hashCode() : 0);
        result = 31 * result + (with != null ? with.hashCode() : 0);
        return result;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public SQLName getRuleName() {
        return ruleName;
    }

    public void setRuleName(SQLName ruleName) {
        this.ruleName = ruleName;
    }

    public SQLName getUserName() {
        return userName;
    }

    public void setUserName(SQLName userName) {
        this.userName = userName;
    }

    public SQLCharExpr getTemplateId() {
        return templateId;
    }

    public void setTemplateId(SQLCharExpr templateId) {
        this.templateId = templateId;
    }

    public SQLListExpr getKeywords() {
        return keywords;
    }

    public void setKeywords(SQLListExpr keywords) {
        this.keywords = keywords;
    }

    public SQLAssignItem getWith() {
        return with;
    }

    public void setWith(SQLAssignItem with) {
        this.with = with;
    }
}
