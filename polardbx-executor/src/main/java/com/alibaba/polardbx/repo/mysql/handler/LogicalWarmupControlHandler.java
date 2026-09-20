package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.CancelWarmupTaskSyncAction;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.scheduler.ColumnarWarmupAccessor;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalWarmupControl;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlWarmupControl;

import java.sql.Connection;
import java.sql.SQLException;

public class LogicalWarmupControlHandler extends HandlerCommon {
    private static final Logger LOGGER = LoggerFactory.getLogger("mpp_log");

    public LogicalWarmupControlHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        Preconditions.checkArgument(logicalPlan instanceof LogicalWarmupControl);
        SqlWarmupControl sqlWarmupControl = ((LogicalWarmupControl) logicalPlan).getSqlWarmupControl();

        ArrayResultCursor result = new ArrayResultCursor("warmup control");
        result.addColumn("TASK_ID", DataTypes.VarcharType);
        result.addColumn("DELETE_STATUS", DataTypes.VarcharType);

        String instId = InstIdUtil.getInstId();
        Object[] row = null;

        SqlWarmupControl.SqlWarmupControlType controlType = sqlWarmupControl.getControlType();
        switch (controlType) {
        case DELETE: {
            if (sqlWarmupControl.isAll()) {
                try (Connection connection = MetaDbUtil.getConnection()) {
                    ColumnarWarmupAccessor columnarWarmupAccessor = new ColumnarWarmupAccessor();
                    columnarWarmupAccessor.setConnection(connection);

                    int affectedRows = columnarWarmupAccessor.deleteAll(instId);

                    row = new Object[] {"ALL", affectedRows + " TASKS DELETED"};
                } catch (SQLException e) {
                    LOGGER.error(e);
                    row = new Object[] {"ALL", e.getMessage()};
                }
            } else {
                long taskId = sqlWarmupControl.getTaskId();
                try (Connection connection = MetaDbUtil.getConnection()) {
                    ColumnarWarmupAccessor columnarWarmupAccessor = new ColumnarWarmupAccessor();
                    columnarWarmupAccessor.setConnection(connection);
                    int affectedRows = columnarWarmupAccessor.deleteWithTaskId(instId, taskId);

                    row = new Object[] {String.valueOf(taskId), affectedRows + " TASKS DELETED"};
                } catch (SQLException e) {
                    LOGGER.error(e);
                    row = new Object[] {String.valueOf(taskId), e.getMessage()};
                }
            }

            result.addRow(row);

            // cancel current task
            ISyncAction cancelAction =
                new CancelWarmupTaskSyncAction(sqlWarmupControl.getTaskId(), sqlWarmupControl.isAll());
            SyncManagerHelper.syncThrowExceptions(cancelAction, SyncScope.CURRENT_ONLY);

            return result;
        }

        case RESUME: {
            if (sqlWarmupControl.isAll()) {
                try (Connection connection = MetaDbUtil.getConnection()) {
                    ColumnarWarmupAccessor columnarWarmupAccessor = new ColumnarWarmupAccessor();
                    columnarWarmupAccessor.setConnection(connection);

                    int affectedRows = columnarWarmupAccessor.resumeAll(instId);

                    row = new Object[] {"ALL", affectedRows + " TASKS RESUMED"};
                } catch (SQLException e) {
                    LOGGER.error(e);
                    row = new Object[] {"ALL", e.getMessage()};
                }
            } else {
                long taskId = sqlWarmupControl.getTaskId();
                try (Connection connection = MetaDbUtil.getConnection()) {
                    ColumnarWarmupAccessor columnarWarmupAccessor = new ColumnarWarmupAccessor();
                    columnarWarmupAccessor.setConnection(connection);
                    int affectedRows = columnarWarmupAccessor.resumeWithTaskId(instId, taskId);

                    row = new Object[] {String.valueOf(taskId), affectedRows + " TASKS RESUMED"};
                } catch (SQLException e) {
                    LOGGER.error(e);
                    row = new Object[] {String.valueOf(taskId), e.getMessage()};
                }
            }

            result.addRow(row);
            return result;
        }

        case SUSPEND: {
            if (sqlWarmupControl.isAll()) {
                try (Connection connection = MetaDbUtil.getConnection()) {
                    ColumnarWarmupAccessor columnarWarmupAccessor = new ColumnarWarmupAccessor();
                    columnarWarmupAccessor.setConnection(connection);

                    int affectedRows = columnarWarmupAccessor.suspendAll(instId);

                    row = new Object[] {"ALL", affectedRows + " TASKS SUSPEND"};
                } catch (SQLException e) {
                    LOGGER.error(e);
                    row = new Object[] {"ALL", e.getMessage()};
                }
            } else {
                long taskId = sqlWarmupControl.getTaskId();
                try (Connection connection = MetaDbUtil.getConnection()) {
                    ColumnarWarmupAccessor columnarWarmupAccessor = new ColumnarWarmupAccessor();
                    columnarWarmupAccessor.setConnection(connection);
                    int affectedRows = columnarWarmupAccessor.suspendWithTaskId(instId, taskId);

                    row = new Object[] {String.valueOf(taskId), affectedRows + " TASKS SUSPEND"};
                } catch (SQLException e) {
                    LOGGER.error(e);
                    row = new Object[] {String.valueOf(taskId), e.getMessage()};
                }
            }

            result.addRow(row);

            // cancel current task
            ISyncAction cancelAction =
                new CancelWarmupTaskSyncAction(sqlWarmupControl.getTaskId(), sqlWarmupControl.isAll());
            SyncManagerHelper.syncThrowExceptions(cancelAction, SyncScope.CURRENT_ONLY);

            return result;
        }

        }

        return result;
    }
}
