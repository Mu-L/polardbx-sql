package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.ScheduleJobStarter;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlFireSchedule;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;
import org.mockito.MockedStatic;

import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_SPM;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class LogicalFireScheduleHandlerTest {

    @Test
    public void testFireSchedule() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        LogicalFireScheduleHandler handler = new LogicalFireScheduleHandler(null);
        ExecutionContext ec = new ExecutionContext();

        SqlFireSchedule sqlFireSchedule = new SqlFireSchedule(SqlParserPos.ZERO, 1L);
        LogicalDal dal = mock(LogicalDal.class);
        when(dal.getNativeSqlNode()).thenReturn(sqlFireSchedule);

        ScheduledJobsRecord scheduledJobsRecord = new ScheduledJobsRecord();
        scheduledJobsRecord.setScheduleId(1L);
        scheduledJobsRecord.setExecutorType("BASELINE_SYNC");
        try (MockedStatic<ScheduleJobStarter> mockedStaticJobStarter = mockStatic(ScheduleJobStarter.class);
            MockedStatic<ScheduledJobsManager> mockedStaticScheduledJobsManager = mockStatic(
                ScheduledJobsManager.class);
            MockedStatic<InstConfUtil> mockedStaticInstConfUtil = mockStatic(InstConfUtil.class)) {
            mockedStaticScheduledJobsManager.when(() -> ScheduledJobsManager.queryScheduledJobById(1L))
                .thenReturn(scheduledJobsRecord);

            handler.handle(dal, ec);

            mockedStaticInstConfUtil.verify(() -> InstConfUtil.getBool(ENABLE_SPM), times(1));
        }
    }
}
