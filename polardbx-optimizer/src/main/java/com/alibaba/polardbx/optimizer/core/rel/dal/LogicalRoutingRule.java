package com.alibaba.polardbx.optimizer.core.rel.dal;

import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.sql.SqlDal;

public class LogicalRoutingRule extends LogicalDal {
    private LogicalRoutingRule(Dal dal) {
        super(dal, "", "", null);
    }

    public static LogicalRoutingRule create(Dal dal) {
        return new LogicalRoutingRule(dal);
    }

    public SqlDal getSqlDal() {
        return (SqlDal) getNativeSqlNode();
    }

    @Override
    protected String getExplainName() {
        return "LogicalRoutingRule";
    }

}
