package com.alibaba.polardbx.optimizer.core.rel.dal;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.sql.SqlWarmupControl;

public class LogicalWarmupControl extends LogicalDal {
    private LogicalWarmupControl(Dal dal) {
        super(dal, "", "", null);
    }

    public static LogicalWarmupControl create(Dal dal) {
        Preconditions.checkArgument(dal.getAst() instanceof SqlWarmupControl);
        return new LogicalWarmupControl(dal);
    }

    public SqlWarmupControl getSqlWarmupControl() {
        return (SqlWarmupControl) getNativeSqlNode();
    }

    @Override
    protected String getExplainName() {
        return "LogicalWarmupControl";
    }
}