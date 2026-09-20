/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.DropJavaFunctionDropMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.DropJavaFunctionSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDropJavaFunctionMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.privilege.PrivilegeKind;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropJavaFunction;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlDropJavaFunction;

import java.util.List;

public class DropJavaFunctionJobFactory extends AbstractFunctionJobFactory {

    private final LogicalDropJavaFunction dropFunction;

    private boolean forceDrop;

    public DropJavaFunctionJobFactory(LogicalDropJavaFunction dropFunction, String schema, boolean forceDrop) {
        super(schema);
        this.dropFunction = dropFunction;
        this.forceDrop = forceDrop;
    }

    @Override
    protected void validate() {
        SqlDropJavaFunction sqlDropFunction = dropFunction.getSqlDropFunction();
        String udfName = sqlDropFunction.getFuncName();
        if (!forceDrop && !sqlDropFunction.isIfExists() && !JavaFunctionManager.getInstance()
            .containsFunction(udfName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UDF_NOT_FOUND,
                String.format("function: %s not exist", udfName));
        }
    }

    @Override
    List<DdlTask> createTasksForOneJob() {
        String functionName = dropFunction.getSqlDropFunction().getFuncName();

        DdlTask dropMetaTask = new DropJavaFunctionDropMetaTask(schema, null, functionName);
        DdlTask cdcMarkTask = new CdcDropJavaFunctionMarkTask(schema, functionName);
        DdlTask syncTask = new DropJavaFunctionSyncTask(schema, functionName);
        return Lists.newArrayList(dropMetaTask, cdcMarkTask, syncTask);
    }

    public static ExecutableDdlJob dropFunction(LogicalDropJavaFunction dropFunction, ExecutionContext ec) {
        if (!ec.isSuperUserOrAllPrivileges() && !PolarPrivilegeUtils.checkPolardbxPrivilege(ec, PrivilegeKind.DROP)) {
            PrivilegeContext pc = ec.getPrivilegeContext();
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                "drop java udf on polardbx", pc.getUser(), pc.getHost());
        }
        boolean forceDropJavaUdf = ec.getParamManager().getBoolean(ConnectionParams.FORCE_DROP_JAVA_UDF);
        SqlDropJavaFunction sqlDropFunction = dropFunction.getSqlDropFunction();
        String javaFuncName = sqlDropFunction.getFuncName();
        boolean isIfExists = sqlDropFunction.isIfExists();
        boolean udfFuncExists = JavaFunctionManager.getInstance().checkIfContainsFunctionByMetaDb(javaFuncName);
        if (!forceDropJavaUdf && !udfFuncExists && isIfExists) {
            return new TransientDdlJob();
        }
        return new DropJavaFunctionJobFactory(dropFunction, ec.getSchemaName(), ec.getParamManager().getBoolean(
            ConnectionParams.FORCE_DROP_JAVA_UDF)).create();
    }
}
