/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.google.common.truth.Truth;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

import java.util.ArrayList;
import java.util.List;

/**
 * Verifies that the flaky methods of ModifyOnTopNTest are really skipped by the
 * JUnit engine after being annotated with @Ignore (AONE-85437638). The target
 * methods are executed through the real JUnit runner so the check reflects the
 * runtime skip behavior instead of source text scanning.
 */
public class ModifyOnTopNIgnoreVerifyTest {

    private static final String FLAKY_CASE_ONE = "testRangeSubpartition";
    private static final String FLAKY_CASE_TWO = "testRangeSubpartition1";

    @Test
    public void testFlakyMethodsAreIgnored() {
        final List<String> ignoredMethods = new ArrayList<>();
        final JUnitCore core = new JUnitCore();
        core.addListener(new RunListener() {
            @Override
            public void testIgnored(Description description) {
                ignoredMethods.add(description.getMethodName());
            }
        });

        final Result result = core.run(ModifyOnTopNTest.class);

        Truth.assertWithMessage(
                "ModifyOnTopNTest flaky methods should be annotated with @Ignore and skipped by JUnit, "
                    + "but actually ignored=" + ignoredMethods
                    + ", runCount=" + result.getRunCount()
                    + ", failureCount=" + result.getFailureCount())
            .that(ignoredMethods)
            .containsAtLeast(FLAKY_CASE_ONE, FLAKY_CASE_TWO);
    }
}
