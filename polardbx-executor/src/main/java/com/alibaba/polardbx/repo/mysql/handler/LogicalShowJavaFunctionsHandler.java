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
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionAccessor;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionMetaRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlShowJavaFunctions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class LogicalShowJavaFunctionsHandler extends HandlerCommon {

    public LogicalShowJavaFunctionsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        if (!executionContext.isSuperUserOrAllPrivileges()) {
            PrivilegeContext pc = executionContext.getPrivilegeContext();
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                "no enough privilege to show java functions", pc.getUser(), pc.getHost());
        }

        SqlShowJavaFunctions show =
            (SqlShowJavaFunctions) ((LogicalShow) logicalPlan).getNativeSqlNode();
        String likePattern = null;
        if (show.like instanceof SqlCharStringLiteral) {
            likePattern = ((SqlCharStringLiteral) show.like).getNlsString().getValue();
        }
        Pattern regex = likePattern != null ? convertLikeToRegex(likePattern) : null;

        ArrayResultCursor result = new ArrayResultCursor("JAVA_FUNCTIONS");
        result.addColumn("FUNCTION_NAME", DataTypes.StringType);
        result.initMeta();

        try (Connection connection = MetaDbUtil.getConnection()) {
            JavaFunctionAccessor accessor = new JavaFunctionAccessor();
            accessor.setConnection(connection);
            List<JavaFunctionMetaRecord> records = accessor.queryAllFunctionMetas();
            List<String> names = new ArrayList<>();
            for (JavaFunctionMetaRecord record : records) {
                String name = record.funcName.toLowerCase();
                if (regex != null && !regex.matcher(name).matches()) {
                    continue;
                }
                names.add(name);
            }
            Collections.sort(names);
            for (String name : names) {
                result.addRow(new Object[] {name});
            }
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                "Failed to query java functions");
        }

        return result;
    }

    public static Pattern convertLikeToRegex(String likePattern) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < likePattern.length(); i++) {
            char c = likePattern.charAt(i);
            if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append('.');
            } else if ("\\[]{}().*+?^$|".indexOf(c) >= 0) {
                sb.append('\\').append(c);
            } else {
                sb.append(c);
            }
        }
        return Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE);
    }
}
