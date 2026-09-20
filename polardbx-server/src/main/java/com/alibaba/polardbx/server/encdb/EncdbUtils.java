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

package com.alibaba.polardbx.server.encdb;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.server.executor.utils.MysqlDefs;

/**
 * @author pangzhaoxing
 */
public class EncdbUtils {

    /**
     * @param sqlType java.sql.Types
     * @return jdbc protocol type
     */
    public static int sqlType2MysqlType(int sqlType, int scale) {
        int mysqlType;
        if (sqlType != DataType.UNDECIDED_SQL_TYPE) {
            mysqlType = MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(sqlType, scale));
        } else {
            mysqlType = MysqlDefs.FIELD_TYPE_STRING; // 默认设置为string
        }
        return mysqlType;
    }

    /**
     * @param encjdbcVersion x.x.x
     */
    public static int compareEncjdbcVersion(String encjdbcVersion, String version) {
        if (encjdbcVersion.endsWith("-SNAPSHOT")) {
            encjdbcVersion = encjdbcVersion.substring(0, encjdbcVersion.length() - 9);
        }
        if (version.endsWith("-SNAPSHOT")) {
            version = version.substring(0, version.length() - 9);
        }
        String[] version1 = encjdbcVersion.split("\\.");
        String[] version2 = version.split("\\.");
        if (version1.length != version2.length) {
            return -1;
        }
        for (int i = 0; i < version1.length; i++) {
            if (Integer.parseInt(version1[i]) > Integer.parseInt(version2[i])) {
                return 1;
            } else if (Integer.parseInt(version1[i]) < Integer.parseInt(version2[i])) {
                return -1;
            }
        }
        return 0;
    }

    public static boolean checkEncjdbcKmsVersion(String encjdbcVersion) {
        String version = InstConfUtil.getOriginVal(ConnectionParams.ENCJDBC_KMS_MIN_VERSION);
        if (version.isEmpty()) {
            return true;
        }
        if (encjdbcVersion == null) {
            return false;
        }
        return
            compareEncjdbcVersion(encjdbcVersion, version)
                >= 0;
    }

}
