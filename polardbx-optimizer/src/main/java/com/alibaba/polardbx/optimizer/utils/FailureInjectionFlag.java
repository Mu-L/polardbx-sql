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

package com.alibaba.polardbx.optimizer.utils;

public class FailureInjectionFlag {

    public final boolean failDuringPrimaryCommit;
    public final boolean failBeforePrimaryCommit;
    public final boolean failAfterPrimaryCommit;
    public final boolean delayBeforeWriteCommitLog;
    public final boolean acFlag;
    public final boolean acFlag2;
    public final boolean acFlag3;
    public final boolean acFlag4;
    public final boolean acFlag5;
    public final boolean acFlag6;
    public final boolean acFlag7;
    public final boolean acFlag8;
    public final boolean acFlag9;
    public final boolean acFlag10;
    public final boolean acFlag11;
    public final boolean acFlag12;
    public final boolean acFlag13;
    public final boolean acFlag14;
    public final boolean acFlag15;
    public final boolean acFlag16;
    public final boolean acFlag17;

    private FailureInjectionFlag(boolean f0, boolean f1, boolean f3, boolean f4, boolean acFlag2, boolean acFlag3,
                                 boolean acFlag4, boolean acFlag5, boolean acFlag6, boolean acFlag7, boolean acFlag8,
                                 boolean acFlag9, boolean acFlag10, boolean acFlag11, boolean acFlag12,
                                 boolean acFlag13, boolean acFlag14, boolean acFlag15, boolean acFlag16,
                                 boolean acFlag17) {
        this.failDuringPrimaryCommit = f0;
        this.failBeforePrimaryCommit = f1;
        this.failAfterPrimaryCommit = f3;
        this.delayBeforeWriteCommitLog = f4;
        this.acFlag2 = acFlag2;
        this.acFlag3 = acFlag3;
        this.acFlag4 = acFlag4;
        this.acFlag5 = acFlag5;
        this.acFlag6 = acFlag6;
        this.acFlag7 = acFlag7;
        this.acFlag8 = acFlag8;
        this.acFlag9 = acFlag9;
        this.acFlag10 = acFlag10;
        this.acFlag11 = acFlag11;
        this.acFlag12 = acFlag12;
        this.acFlag13 = acFlag13;
        this.acFlag14 = acFlag14;
        this.acFlag15 = acFlag15;
        this.acFlag16 = acFlag16;
        this.acFlag17 = acFlag17;
        this.acFlag = acFlag2 || acFlag3 || acFlag4 || acFlag5 || acFlag6 || acFlag7 || acFlag8 || acFlag9 || acFlag10
            || acFlag11 || acFlag12 || acFlag13 || acFlag14 || acFlag15 || acFlag16 || acFlag17;
    }

    public static FailureInjectionFlag parseString(String str) {
        return new FailureInjectionFlag(
            str.contains("FAIL_DURING_PRIMARY_COMMIT"),
            str.contains("FAIL_BEFORE_PRIMARY_COMMIT"),
            str.contains("FAIL_AFTER_PRIMARY_COMMIT"),
            str.contains("DELAY_BEFORE_WRITE_COMMIT_LOG"),
            str.contains("AC_FLAG_2"),
            str.contains("AC_FLAG_3"),
            str.contains("AC_FLAG_4"),
            str.contains("AC_FLAG_5"),
            str.contains("AC_FLAG_6"),
            str.contains("AC_FLAG_7"),
            str.contains("AC_FLAG_8"),
            str.contains("AC_FLAG_9"),
            str.contains("AC_FLAG_10"),
            str.contains("AC_FLAG_11"),
            str.contains("AC_FLAG_12"),
            str.contains("AC_FLAG_13"),
            str.contains("AC_FLAG_14"),
            str.contains("AC_FLAG_15"),
            str.contains("AC_FLAG_16"),
            str.contains("AC_FLAG_17")
        );
    }

    public static final FailureInjectionFlag EMPTY =
        new FailureInjectionFlag(false, false, false, false, false, false, false,
            false, false, false, false, false, false, false,
            false, false, false, false, false, false);
}
