package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.server.ServerConnection;

/**
 * @author yaozhili
 */
public abstract class BaseInnerProcedure {

    /**
     * @param cursor results returned to user
     */
    abstract void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor);

    /**
     * 默认使用ArrayResultCursor全量缓存结果；
     * 结果集可能很大的procedure可覆写此方法返回自定义cursor（如支持spill的cursor）
     */
    public ResultCursor getResultCursor(ServerConnection c, SQLCallStatement statement, String procedureName) {
        ArrayResultCursor cursor = new ArrayResultCursor(procedureName);
        execute(c, statement, cursor);
        return cursor;
    }
}
