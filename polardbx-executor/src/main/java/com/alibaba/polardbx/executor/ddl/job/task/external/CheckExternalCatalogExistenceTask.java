package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "CheckExternalCatalogExistenceTask")
public class CheckExternalCatalogExistenceTask extends BaseValidateTask {

    private final String catalogName;
    private final boolean ifExists;

    public CheckExternalCatalogExistenceTask(String catalogName, boolean ifExists) {
        super(SystemDbHelper.DEFAULT_DB_NAME);
        this.catalogName = catalogName;
        this.ifExists = ifExists;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor(metaDbConn);
            if (accessor.selectByName(catalogName) == null) {
                if (!ifExists) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                        "External catalog '" + catalogName + "' does not exist");
                }
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                "Validation failed: " + e.getMessage());
        }
    }
}
