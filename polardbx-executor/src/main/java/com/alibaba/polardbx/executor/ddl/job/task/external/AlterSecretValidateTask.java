package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "AlterSecretValidateTask")
public class AlterSecretValidateTask extends BaseValidateTask {

    private final String secretName;

    public AlterSecretValidateTask(String secretName) {
        super(SystemDbHelper.DEFAULT_DB_NAME);
        this.secretName = secretName;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ExternalSecretAccessor accessor = new ExternalSecretAccessor(metaDbConn);
            if (accessor.selectByName(secretName) == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                    "Secret '" + secretName + "' does not exist");
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                "Validation failed: " + e.getMessage());
        }
    }
}
