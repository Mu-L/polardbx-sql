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

import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mockStatic;

public class CacheUserAccessorTest {

    @Test
    public void testInsertUser() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger insertCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> insertCount.get());

            CacheUserAccessor accessor = new CacheUserAccessor();

            // Successful insert
            insertCount.set(1);
            boolean result = accessor.insertUser("user1", "enc_pwd", 1, 1, 0);
            Assert.assertTrue(result);

            // Insert with 0 affected rows
            insertCount.set(0);
            result = accessor.insertUser("user2", "enc_pwd2", 1, 0, 0);
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testInsertUserDuplicate() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new SQLIntegrityConstraintViolationException("Duplicate entry"));

            CacheUserAccessor accessor = new CacheUserAccessor();
            boolean result = accessor.insertUser("user1", "enc_pwd", 1, 1, 0);
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testGetByUserName() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<CacheUserRecord> recordList = new ArrayList<>();
            CacheUserRecord record = new CacheUserRecord();
            record.id = 1L;
            record.userName = "admin";
            record.password = "enc_pwd";
            record.readPriv = 1;
            record.writePriv = 1;
            record.adminPriv = 1;
            recordList.add(record);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CacheUserRecord.class), Mockito.any())).thenReturn(recordList);

            CacheUserAccessor accessor = new CacheUserAccessor();
            CacheUserRecord result = accessor.getByUserName("admin");
            Assert.assertNotNull(result);
            Assert.assertEquals("admin", result.userName);
        }
    }

    @Test
    public void testGetByUserNameNotFound() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CacheUserRecord.class), Mockito.any())).thenReturn(Collections.emptyList());

            CacheUserAccessor accessor = new CacheUserAccessor();
            CacheUserRecord result = accessor.getByUserName("nonexistent");
            Assert.assertNull(result);
        }
    }

    @Test
    public void testGetAllUsers() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<CacheUserRecord> recordList = new ArrayList<>();
            recordList.add(new CacheUserRecord());
            recordList.add(new CacheUserRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(),
                Mockito.eq(CacheUserRecord.class), Mockito.any())).thenReturn(recordList);

            CacheUserAccessor accessor = new CacheUserAccessor();
            List<CacheUserRecord> result = accessor.getAllUsers();
            Assert.assertEquals(2, result.size());
        }
    }

    @Test
    public void testUpdatePassword() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> updateCount.get());

            CacheUserAccessor accessor = new CacheUserAccessor();

            updateCount.set(1);
            boolean result = accessor.updatePassword("user1", "new_enc_pwd");
            Assert.assertTrue(result);

            updateCount.set(0);
            result = accessor.updatePassword("nonexistent", "new_enc_pwd");
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testUpdatePrivileges() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> updateCount.get());

            CacheUserAccessor accessor = new CacheUserAccessor();

            updateCount.set(1);
            boolean result = accessor.updatePrivileges("user1", 1, 1, 1);
            Assert.assertTrue(result);

            updateCount.set(0);
            result = accessor.updatePrivileges("nonexistent", 1, 0, 0);
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testDeleteUser() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger deleteCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> deleteCount.get());

            CacheUserAccessor accessor = new CacheUserAccessor();

            deleteCount.set(1);
            boolean result = accessor.deleteUser("user1");
            Assert.assertTrue(result);

            deleteCount.set(0);
            result = accessor.deleteUser("nonexistent");
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testGetByUserNameError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CacheUserRecord.class), Mockito.any())).thenThrow(new RuntimeException("mock error"));

            CacheUserAccessor accessor = new CacheUserAccessor();
            try {
                accessor.getByUserName("user");
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testGetAllUsersError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(),
                Mockito.eq(CacheUserRecord.class), Mockito.any())).thenThrow(new RuntimeException("mock error"));

            CacheUserAccessor accessor = new CacheUserAccessor();
            try {
                accessor.getAllUsers();
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }
}
