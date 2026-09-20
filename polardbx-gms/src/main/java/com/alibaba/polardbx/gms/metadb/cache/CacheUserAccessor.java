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

package com.alibaba.polardbx.gms.metadb.cache;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.CACHE_USER;

public class CacheUserAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheUserAccessor.class);
    private static final String CACHE_USER_TABLE = wrap(CACHE_USER);

    private static final String INSERT_USER = "insert into " + CACHE_USER_TABLE
        + " (`user_name`, `password`, `read_priv`, `write_priv`, `admin_priv`) values (?, ?, ?, ?, ?)";

    private static final String SELECT_BY_USER_NAME = "select * from " + CACHE_USER_TABLE
        + " where `user_name` = ?";

    private static final String SELECT_ALL = "select * from " + CACHE_USER_TABLE;

    private static final String UPDATE_PASSWORD = "update " + CACHE_USER_TABLE
        + " set `password` = ? where `user_name` = ?";

    private static final String UPDATE_PRIV = "update " + CACHE_USER_TABLE
        + " set `read_priv` = ?, `write_priv` = ?, `admin_priv` = ? where `user_name` = ?";

    private static final String DELETE_USER = "delete from " + CACHE_USER_TABLE
        + " where `user_name` = ?";

    /**
     * Insert a new cache user. Password should already be encrypted by PasswdUtil.
     *
     * @return true if inserted successfully
     */
    public boolean insertUser(String userName, String encryptedPassword,
                              int readPriv, int writePriv, int adminPriv) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, userName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, encryptedPassword);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, readPriv);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setInt, writePriv);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setInt, adminPriv);

            try {
                final int inserts = MetaDbUtil.insert(INSERT_USER, params, connection);
                return 1 == inserts;
            } catch (SQLIntegrityConstraintViolationException e) {
                // duplicate user name
                return false;
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public CacheUserRecord getByUserName(String userName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, userName);

            List<CacheUserRecord> records =
                MetaDbUtil.query(SELECT_BY_USER_NAME, params, CacheUserRecord.class, connection);
            if (records.isEmpty()) {
                return null;
            }
            return records.get(0);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<CacheUserRecord> getAllUsers() {
        try {
            return MetaDbUtil.query(SELECT_ALL, CacheUserRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public boolean updatePassword(String userName, String encryptedPassword) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, encryptedPassword);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, userName);

            final int updates = MetaDbUtil.update(UPDATE_PASSWORD, params, connection);
            return 1 == updates;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public boolean updatePrivileges(String userName, int readPriv, int writePriv, int adminPriv) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, readPriv);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, writePriv);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, adminPriv);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, userName);

            final int updates = MetaDbUtil.update(UPDATE_PRIV, params, connection);
            return 1 == updates;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public boolean deleteUser(String userName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, userName);

            final int deletes = MetaDbUtil.delete(DELETE_USER, params, connection);
            return 1 == deletes;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
