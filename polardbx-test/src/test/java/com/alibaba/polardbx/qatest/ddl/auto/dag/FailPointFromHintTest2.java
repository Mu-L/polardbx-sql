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

package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

@Ignore
public class FailPointFromHintTest2 {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private ExecutionContext executionContext;

    @Before
    public void before() {
        this.executionContext = new ExecutionContext();
        Map<String, Object> hint = new HashMap<>();
        executionContext.setExtraCmds(hint);
    }

    private void addHint(String key, String value) {
        this.executionContext.getExtraCmds().put(key, value);
    }

    private void clearHint() {
        this.executionContext.getExtraCmds().clear();
    }

    @Test
    public void testInject1() {

        String key = "FailPointFromHintTest2Key1";

        /**
         * haven't register any key, so no error will be thrown
         */
        FailPoint.injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected error message");
        });

        thrown.expect(RuntimeException.class);
        addHint(key, key);
        /**
         * since FailPointFromHintTest2Key1 registered, RuntimeException will be thrown
         */
        FailPoint.injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected error message");
        });
        /**
         * since FailPointFromHintTest2Key2 is not registered, IllegalArgumentException won't be thrown
         */
        FailPoint.injectFromHint("FailPointFromHintTest2Key2", executionContext, () -> {
            throw new IllegalArgumentException("injected error message");
        });

        /**
         * since FailPointFromHintTest2Key1 deRegistered, RuntimeException won't be thrown
         */
        clearHint();
        FailPoint.injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected error message");
        });

    }

    @Test
    public void testInject2() {

        String key = "FailPointFromHintTest2Key1";

        FailPoint.injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected error message");
        });

        addHint("foobar", "");
        FailPoint.injectFromHint(key, executionContext, () -> {
            throw new RuntimeException("injected error message");
        });

        clearHint();
        FailPoint.injectFromHint("foobar", executionContext, () -> {
            throw new RuntimeException("injected error message");
        });
    }

    @Test
    public void testInject3() {

        String key = "FailPointFromHintTest2Key1";

        long start = System.currentTimeMillis();

        addHint(key, "");
        FailPoint.injectFromHint(key, executionContext, () -> {
            try {
                Thread.sleep(3 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        long end = System.currentTimeMillis();

        Assert.assertTrue((end - start) > 2500l);
    }

    @Test
    public void testReadValue() {

        addHint("FailPointFromHintTest2Key1", "value1");
        FailPoint.inject("FailPointFromHintTest2Key1", (k, v) -> {
            Assert.assertEquals(k, "FailPointFromHintTest2Key1");
            Assert.assertEquals(v, "value1");
        });
    }

}