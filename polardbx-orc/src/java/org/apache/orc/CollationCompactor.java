/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * @author lijiu.lzw
 */
public class CollationCompactor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CollationCompactor.class);
    private static Class<?> clazz;
    private static Method constructor;
    private static Method method;

    private static Class<?> sliceClass;
    private static Method sliceMethod;
    private static volatile boolean initialized = false;

    private final Object object;

    private static void init() {
        if (!initialized) {
            synchronized (CollationCompactor.class) {
                if (!initialized) {
                    try {
                        clazz = Class.forName("com.alibaba.polardbx.optimizer.core.datatype.VarcharType");
                        constructor = clazz.getMethod("buildVarcharType", String.class, String.class);
                        method = clazz.getSuperclass().getDeclaredMethod("compare", Object.class, Object.class);
                        sliceClass = Class.forName("io.airlift.slice.Slices");
                        sliceMethod = sliceClass.getDeclaredMethod("wrappedBuffer", byte[].class, int.class, int.class);
                    } catch (Exception e) {
                        LOGGER.error("orc CollationCompactor init failed!", e);
                    }
                    //报错也初始化了，避免一直报错
                    initialized = true;
                }
            }
        }
    }

    public CollationCompactor(String charsetName, String collationName) {
        init();
        try {
            object = constructor.invoke(null, charsetName, collationName);
        } catch (Exception e) {
            throw new RuntimeException("orc CollationCompactor init failed!", e);
        }
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            Object o1 = sliceMethod.invoke(null, b1, s1, l1);
            Object o2 = sliceMethod.invoke(null, b2, s2, l2);
            return (int) method.invoke(object, o1, o2);
        } catch (Exception e) {
            throw new RuntimeException("orc CollationCompactor compare failed!", e);
        }
    }
}
