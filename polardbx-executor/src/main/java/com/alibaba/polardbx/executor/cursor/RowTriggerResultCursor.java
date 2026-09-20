package com.alibaba.polardbx.executor.cursor;

import com.alibaba.polardbx.executor.deeppage.LastRowTrigger;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.concurrent.atomic.AtomicLong;

public class RowTriggerResultCursor extends ResultCursor {

    private Row lastRow;

    private LastRowTrigger lastRowTrigger;

    private AtomicLong rowCount = new AtomicLong(0);

    public RowTriggerResultCursor(ResultCursor resultCursor) {
        super(resultCursor);
        this.setCursorMeta(resultCursor.getCursorMeta());
    }

    @Override
    public Row doNext() {
        Row row = super.doNext();
        if (row != null) {
            lastRow = row;
            rowCount.incrementAndGet();
        } else {
            //如果row==null，说明上一个返回的row是lastRow，那么触发lastRowTrigger
            lastRowTrigger.onLastRow(lastRow, getCursorMeta(), rowCount.get());
        }
        return row;
    }

    public void setLastRowTrigger(LastRowTrigger lastRowTrigger) {
        this.lastRowTrigger = lastRowTrigger;
    }

}
