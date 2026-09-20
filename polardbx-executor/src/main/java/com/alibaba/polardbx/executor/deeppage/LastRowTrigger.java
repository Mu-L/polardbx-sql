package com.alibaba.polardbx.executor.deeppage;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.ResultSet;

public interface LastRowTrigger {

    public void onLastRow(Row row, CursorMeta cursorMeta, long rows);

}
