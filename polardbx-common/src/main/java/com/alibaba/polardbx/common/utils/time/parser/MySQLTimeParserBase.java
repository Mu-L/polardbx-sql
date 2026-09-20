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

package com.alibaba.polardbx.common.utils.time.parser;

public class MySQLTimeParserBase {
    protected static final long MAX_UNSIGNED_INTEGER_VALUE = (long) Integer.MAX_VALUE - (long) Integer.MIN_VALUE;

    public static final boolean TIME_NO_ZERO_IN_DATE = false;

    public static final boolean TIME_NO_ZERO_DATE = false;

    public static final boolean TIME_FUZZY_DATE = true;

    public static final boolean TIME_INVALID_DATES = false;

    public static final boolean TIME_DATETIME_ONLY = false;

    public static final boolean TIME_NO_NANO_ROUNDING = false;

    public static long[] LOG_10 =
        {
            1, 10, 100, 1000, 10000, 100000, 1000000, 10000000,
            100000000L, 1000000000L, 10000000000L, 100000000000L,
            1000000000000L, 10000000000000L, 100000000000000L,
            1000000000000000L, 10000000000000000L, 100000000000000000L,
            1000000000000000000L
        };

    protected static boolean isSpace(byte b) {
        return b == ' ';
    }

    protected static boolean isDigit(byte b) {
        return b >= '0' && b <= '9';
    }

    // Change context:
    // - Before: only '.', ',', '-' and ':' were treated as punctuation in this early
    //   minimal port of MySQL's str_to_datetime, so literals with other separators
    //   (e.g. '2021/08/11') failed to parse and were silently turned into null.
    // - Path impact: shared by StringTimeParser field-separator skipping and the
    //   STR_TO_DATE '.' format token; partition pruning, runtime date/datetime/time
    //   conversion and X-Protocol result decoding all route through here. Digit,
    //   alpha and space classification is unchanged, and inputs that parsed before
    //   still parse identically.
    // - Capability regression: None; the widened set matches MySQL my_ispunct exactly
    //   (0x21-0x2F, 0x3A-0x40, 0x5B-0x60, 0x7B-0x7E), restoring MySQL-compatible
    //   parsing without adding any new error behavior.
    protected static boolean isPunctuation(byte b) {
        int c = Byte.toUnsignedInt(b);
        return (c >= 0x21 && c <= 0x2F)
            || (c >= 0x3A && c <= 0x40)
            || (c >= 0x5B && c <= 0x60)
            || (c >= 0x7B && c <= 0x7E);
    }

    protected static boolean isAlpha(byte b) {
        return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z');
    }

    protected static int toAlphaUpper(byte b) {
        int diff = (b >= 'a' && b <= 'z') ? 32 : 0;
        return Byte.toUnsignedInt(b) - diff;
    }
}
