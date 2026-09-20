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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

public final class Assert {

    public static void assertNotNull(Object object) {
        assertNotNull(object, null);
    }

    public static void assertNotNull(Object object, String message) {
        if (object == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_NULL, message);
        }
    }

    public static void assertTrue(boolean expression) {
        assertTrue(expression, null);
    }

    public static void assertEqual(String expected, String actual) {
        if (TStringUtil.isEmpty(expected) || TStringUtil.isEmpty(actual)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_NULL, "expected:" + expected + ", actual:" + actual);
        }
        if (!expected.equalsIgnoreCase(actual)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_NULL, "expected:" + expected + ", actual:" + actual);
        }
    }

    public static void assertEqual(int expected, int actual) {
        if (expected != actual) {
            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_NULL, "expected:" + expected + ", actual:" + actual);
        }
    }

    public static void assertEqual(boolean expected, boolean actual) {
        if (expected != actual) {
            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_NULL, "expected:" + expected + ", actual:" + actual);
        }
    }

    /**
     * 确保表达式为真，否则抛出<code>TddlRuntimeException + ErrorCode.ERR_ASSERT_TRUE</code>
     */
    public static void assertTrue(boolean expression, String message) {
        if (!expression) {
            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_TRUE, message);
        }
    }

    public static void fail() {
        fail(null);
    }

    public static void fail(String message) {
        throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL, message);
    }

    public static void check(boolean expression) {
        if (!expression) {
            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_TRUE);
        }
    }


}
