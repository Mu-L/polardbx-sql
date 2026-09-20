package com.alibaba.polardbx.executor.ddl.job.task.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfoRecord;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "DropSecretValidateTask")
public class DropSecretValidateTask extends BaseValidateTask {

    private final String secretName;
    private final boolean ifExists;

    public DropSecretValidateTask(String secretName, boolean ifExists) {
        super(SystemDbHelper.DEFAULT_DB_NAME);
        this.secretName = secretName;
        this.ifExists = ifExists;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            // Check if secret exists
            ExternalSecretAccessor secretAccessor = new ExternalSecretAccessor(metaDbConn);
            ExternalSecretRecord record = secretAccessor.selectByName(secretName);
            if (record == null) {
                if (!ifExists) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                        "Secret '" + secretName + "' does not exist");
                }
                return;
            }

            // Check catalog references
            ExternalCatalogInfoAccessor catAccessor = new ExternalCatalogInfoAccessor();
            catAccessor.setConnection(metaDbConn);
            List<ExternalCatalogInfoRecord> refs = catAccessor.selectBySecretName(secretName);
            if (!refs.isEmpty()) {
                String names = refs.stream()
                    .map(ExternalCatalogInfoRecord::getName)
                    .collect(Collectors.joining(", "));
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                    "Cannot drop secret '" + secretName + "': referenced by catalog(s) [" + names + "]");
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                "Validation failed: " + e.getMessage());
        }
    }
}
