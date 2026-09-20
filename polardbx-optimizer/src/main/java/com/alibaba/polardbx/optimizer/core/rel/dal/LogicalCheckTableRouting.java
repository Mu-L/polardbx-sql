package com.alibaba.polardbx.optimizer.core.rel.dal;

import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.sql.SqlBaseline;
import org.apache.calcite.sql.SqlCheckTableRouting;

public class LogicalCheckTableRouting extends LogicalDal {

    private LogicalCheckTableRouting(Dal dal){
        super(dal, "", "", null);
    }

    public static LogicalCheckTableRouting create(Dal dal) {
        assert dal.getAst() instanceof SqlCheckTableRouting;
        return new LogicalCheckTableRouting(dal);
    }

    @Override
    protected String getExplainName() {
        return "LogicalCheckTableRouting";
    }
}

