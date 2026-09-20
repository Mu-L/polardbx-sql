package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.misc.DdlInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDdlInfo;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDdlProgress;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author wumu
 */
public class InformationSchemaDdlInfoHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaDdlInfoHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaDdlInfo;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        DdlJobManager ddlJobManager = new DdlJobManager();

        final int jobIdIndex = InformationSchemaDdlProgress.getJobIdIndex();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        Set<String> jobIds = virtualView.getEqualsFilterValues(jobIdIndex, params);

        if (CollectionUtils.isNotEmpty(jobIds)) {
            for (String jobId : jobIds) {
                DdlInfoRecord ddlRecord = ddlJobManager.fetchDdlInfoRecordByJobId(Long.parseLong(jobId));
                if (ddlRecord == null) {
                    ddlRecord = ddlJobManager.fetchDdlInfoArchiveRecordByJobId(Long.parseLong(jobId));
                }
                if (ddlRecord == null) {
                    continue;
                }
                addRow(cursor, ddlRecord.jobId, ddlRecord.state, ddlRecord.ddlStmt, new Timestamp(ddlRecord.gmtCreated),
                    new Timestamp(ddlRecord.gmtModified));
            }
        } else {
            List<DdlInfoRecord> ddlRecordList = ddlJobManager.fetchAllCurrentDdlInfoRecord();
            List<DdlInfoRecord> ddlRecordArchiveList = ddlJobManager.fetchAllArchiveDdlInfoRecord();
            ddlRecordList.addAll(ddlRecordArchiveList);

            for (DdlInfoRecord ddlRecord : ddlRecordList) {
                addRow(cursor, ddlRecord.jobId, ddlRecord.state, ddlRecord.ddlStmt, new Timestamp(ddlRecord.gmtCreated),
                    new Timestamp(ddlRecord.gmtModified));
            }
        }

        return cursor;
    }

    private static void addRow(ArrayResultCursor cursor, long jobId, String state, String ddlStmt,
                               Timestamp startTime, Timestamp endTime) {
        cursor.addRow(new Object[] {jobId, state, ddlStmt, startTime, endTime});
    }
}
