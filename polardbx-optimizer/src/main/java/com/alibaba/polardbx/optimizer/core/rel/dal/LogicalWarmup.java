package com.alibaba.polardbx.optimizer.core.rel.dal;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.sql.SqlWarmup;

public class LogicalWarmup extends LogicalDal {
    private LogicalWarmup(Dal dal){
        super(dal, "", "", null);
    }

    public static LogicalWarmup create(Dal dal) {
        Preconditions.checkArgument(dal.getAst() instanceof SqlWarmup);
        return new LogicalWarmup(dal);
    }

    public SqlWarmup getSqlWarmup() {
        return (SqlWarmup) getNativeSqlNode();
    }

    @Override
    protected String getExplainName() {
        return "LogicalWarmup";
    }
}
