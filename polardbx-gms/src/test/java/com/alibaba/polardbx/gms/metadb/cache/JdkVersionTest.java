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

import com.alibaba.polardbx.cache.utils.Jdk9Compat;
import org.junit.Assert;
import org.junit.Test;

public class JdkVersionTest {
    @Test
    public void testJdkVersion() {
        final int version = Jdk9Compat.jdkVersion();
        final int runtimeVersion = getMajorJavaVersion();

        if (runtimeVersion >= 11) {
            Assert.assertEquals("On JDK 11+, Multi-Release should load JDK 11 impl", 11, version);
        } else {
            Assert.assertEquals("On JDK 8, fallback impl should be loaded", 8, version);
        }

        System.out.println("Runtime JDK: " + runtimeVersion + ", Jdk9Compat.jdkVersion(): " + version);
    }

    private static int getMajorJavaVersion() {
        String specVersion = System.getProperty("java.specification.version");
        if (specVersion.startsWith("1.")) {
            // e.g. "1.8" -> 8
            return Integer.parseInt(specVersion.substring(2));
        }
        // e.g. "11", "17" -> 11, 17
        return Integer.parseInt(specVersion);
    }
}
