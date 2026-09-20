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
package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionAccessor;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowCreateJavaFunction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class LogicalShowCreateJavaFunctionHandler extends HandlerCommon {

    public LogicalShowCreateJavaFunctionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        if (!executionContext.isSuperUserOrAllPrivileges()) {
            PrivilegeContext pc = executionContext.getPrivilegeContext();
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                "no enough privilege to show create java function", pc.getUser(), pc.getHost());
        }

        SqlShowCreateJavaFunction showNode =
            (SqlShowCreateJavaFunction) ((LogicalShow) logicalPlan).getNativeSqlNode();
        String funcName = RelUtils.lastStringValue(showNode.getFunctionName()).toLowerCase();

        ArrayResultCursor result = new ArrayResultCursor("JAVA_FUNCTION");
        result.addColumn("Java Function", DataTypes.StringType);
        result.addColumn("Create Java Function", DataTypes.StringType);
        result.initMeta();

        try (Connection connection = MetaDbUtil.getConnection()) {
            JavaFunctionAccessor accessor = new JavaFunctionAccessor();
            accessor.setConnection(connection);
            List<JavaFunctionRecord> records = accessor.queryFunctionByName(funcName);

            if (records.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_UDF_NOT_FOUND, funcName);
            }

            JavaFunctionRecord record = records.get(0);
            String createStatement = buildCreateStatement(funcName, record);
            result.addRow(new Object[] {funcName, createStatement});
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                "Failed to query java function: " + funcName);
        }

        return result;
    }

    private String buildCreateStatement(String funcName, JavaFunctionRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELIMITER $$\n");
        sb.append("CREATE JAVA FUNCTION IF NOT EXISTS `").append(funcName).append("`\n");

        if (record.noState != null && record.noState) {
            sb.append("  NO STATE\n");
        }

        if (TStringUtil.isNotBlank(record.returnType)) {
            sb.append("  RETURN_TYPE ").append(record.returnType).append("\n");
        }

        if (TStringUtil.isNotBlank(record.inputTypes)) {
            sb.append("  INPUT_TYPES ").append(record.inputTypes).append("\n");
        }

        sb.append("  CODE\n");
        sb.append(record.code).append("\n");
        sb.append("END_CODE $$\n");
        sb.append("DELIMITER ;");

        return sb.toString();
    }
}
