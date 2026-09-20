/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Partbylong extends UserDefinedJavaFunction {
    protected int[] count;
    protected int[] length;
    private PartitionUtil partitionUtil;

    public static class SplitUtil {
        private SplitUtil() {
        }

        private static final String[] EMPTY_STRING_ARRAY = new String[0];

        public static String[] split(String src) {
            return split(src, null, -1);
        }

        public static String[] split(String src, char separatorChar) {
            if (src == null) {
                return null;
            }
            int length = src.length();
            if (length == 0) {
                return EMPTY_STRING_ARRAY;
            }
            List<String> list = new LinkedList<>();
            int i = 0;
            int start = 0;
            boolean match = false;
            while (i < length) {
                if (src.charAt(i) == separatorChar) {
                    if (match) {
                        list.add(src.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
            if (match) {
                list.add(src.substring(start, i));
            }
            return list.toArray(new String[list.size()]);
        }

        public static String[] split(String src, char separatorChar, boolean trim) {
            if (src == null) {
                return null;
            }
            int length = src.length();
            if (length == 0) {
                return EMPTY_STRING_ARRAY;
            }
            List<String> list = new LinkedList<>();
            int i = 0;
            int start = 0;
            boolean match = false;
            while (i < length) {
                if (src.charAt(i) == separatorChar) {
                    if (match) {
                        if (trim) {
                            list.add(src.substring(start, i).trim());
                        } else {
                            list.add(src.substring(start, i));
                        }
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
            if (match) {
                if (trim) {
                    list.add(src.substring(start, i).trim());
                } else {
                    list.add(src.substring(start, i));
                }
            }
            return list.toArray(new String[list.size()]);
        }

        public static String[] split(String str, String separatorChars) {
            return split(str, separatorChars, -1);
        }

        public static String[] split(String src, String separatorChars, int max) {
            if (src == null) {
                return null;
            }
            int length = src.length();
            if (length == 0) {
                return EMPTY_STRING_ARRAY;
            }
            List<String> list = new LinkedList<>();
            int sizePlus1 = 1;
            int i = 0;
            int start = 0;
            boolean match = false;
            if (separatorChars == null) { // null means use whitespace as Separator
                while (i < length) {
                    if (java.lang.Character.isWhitespace(src.charAt(i))) {
                        if (match) {
                            if (sizePlus1++ == max) {
                                i = length;
                            }
                            list.add(src.substring(start, i));
                            match = false;
                        }
                        start = ++i;
                        continue;
                    }
                    match = true;
                    i++;
                }
            } else if (separatorChars.length() == 1) {
                char sep = separatorChars.charAt(0);
                while (i < length) {
                    if (src.charAt(i) == sep) {
                        if (match) {
                            if (sizePlus1++ == max) {
                                i = length;
                            }
                            list.add(src.substring(start, i));
                            match = false;
                        }
                        start = ++i;
                        continue;
                    }
                    match = true;
                    i++;
                }
            } else {
                while (i < length) {
                    if (separatorChars.indexOf(src.charAt(i)) >= 0) {
                        if (match) {
                            if (sizePlus1++ == max) {
                                i = length;
                            }
                            list.add(src.substring(start, i));
                            match = false;
                        }
                        start = ++i;
                        continue;
                    }
                    match = true;
                    i++;
                }
            }
            if (match) {
                list.add(src.substring(start, i));
            }
            return list.toArray(new String[list.size()]);
        }

        /**
         * parser String,eg: <br>
         * 1. c1='$',c2='-',c3='[',c4=']' input:mysql_db$0-2<br>
         * output:mysql_db[0],mysql_db[1],mysql_db[2]<br>
         * 2. c1='$',c2='-',c3='#',c4='0' input:mysql_db$0-2<br>
         * output:mysql_db#0,mysql_db#1,mysql_db#2<br>
         * 3. c1='$',c2='-',c3='0',c4='0' input:mysql_db$0-2<br>
         * output:mysql_db0,mysql_db1,mysql_db2<br>
         */
        public static String[] split(String src, char c1, char c2, char c3, char c4) {
            if (src == null) {
                return null;
            }
            int length = src.length();
            if (length == 0) {
                return EMPTY_STRING_ARRAY;
            }
            List<String> list = new LinkedList<>();
            if (src.indexOf(c1) == -1) {
                list.add(src.trim());
            } else {
                String[] s = split(src, c1, true);
                String[] scope = split(s[1], c2, true);
                int min = Integer.parseInt(scope[0]);
                int max = Integer.parseInt(scope[scope.length - 1]);
                if (c3 == '0') {
                    for (int x = min; x <= max; x++) {
                        list.add(s[0] + x);
                    }
                } else if (c4 == '0') {
                    for (int x = min; x <= max; x++) {
                        list.add(s[0] + c3 + x);
                    }
                } else {
                    for (int x = min; x <= max; x++) {
                        list.add(s[0] + c3 + x + c4);
                    }
                }
            }
            return list.toArray(new String[list.size()]);
        }

        public static String[] split(String src, char fi, char se, char th) {
            return split(src, fi, se, th, '0', '0');
        }

        public static String[] split(String src, char fi, char se, char th, char left, char right) {
            List<String> list = new LinkedList<>();
            String[] pools = split(src, fi, true);
            for (String pool : pools) {
                if (pool.indexOf(se) == -1) {
                    list.add(pool);
                    continue;
                }
                String[] s = split(pool, se, th, left, right);
                Collections.addAll(list, s);
            }
            return list.toArray(new String[list.size()]);
        }

    }

    public static class StringUtil {
        private StringUtil() {
        }

        /**
         * String hash:s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1] <br>
         * h = 31*h + s.charAt(i); => h = (h << 5) - h + s.charAt(i); <br>
         *
         * @param start hash for s.substring(start, end)
         * @param end hash for s.substring(start, end)
         */
        public static long hash(String s, int start, int end) {
            if (start < 0) {
                start = 0;
            }
            if (end > s.length()) {
                end = s.length();
            }
            long h = 0;
            for (int i = start; i < end; ++i) {
                h = (h << 5) - h + s.charAt(i);
            }
            return h;
        }
    }

    public static class PartitionUtil {

        // MAX_PARTITION_LENGTH: if the number is 2^n,  then optimizer by x % 2^n == x & (2^n - 1).
        private static final int MAX_PARTITION_LENGTH = 2880;
        private int partitionLength;

        // cached the value of  2^n - 1 because of x % 2^n == x & (2^n - 1).
        private long addValue;

        private int[] segment;

        private boolean canProfile = false;

        private int segmentLength = 0;

        public PartitionUtil(int[] count, int[] length) {
            if (count == null || length == null || (count.length != length.length)) {
                throw new RuntimeException("error,check your partitionCount & partitionLength definition.");
            }
            for (int iLength : length) {
                if (iLength <= 0) {
                    throw new RuntimeException("error,make sure your partitionLength at least 1.");
                }
            }
            for (int aCount : count) {
                if (aCount <= 0) {
                    throw new RuntimeException("error,make sure your partitionCount at least 1.");
                }
                segmentLength += aCount;
            }
            int[] ai = new int[segmentLength + 1];

            int index = 0;
            for (int i = 0; i < count.length; i++) {
                for (int j = 0; j < count[i]; j++) {
                    ai[++index] = ai[index - 1] + length[i];
                }
            }
            partitionLength = ai[ai.length - 1];
            addValue = partitionLength - 1;
            segment = new int[partitionLength];
            if (partitionLength > MAX_PARTITION_LENGTH) {
                throw new RuntimeException(
                    "error,check your partitionScope definition.Sum(count[i]*length[i]) must be less than "
                        + MAX_PARTITION_LENGTH);
            }
            if ((partitionLength & addValue) == 0) {
                canProfile = true;
            }

            for (int i = 1; i < ai.length; i++) {
                for (int j = ai[i - 1]; j < ai[i]; j++) {
                    segment[j] = (i - 1);
                }
            }
        }

        public int partition(long hash) {
            if (canProfile) {
                return segment[(int) (hash & addValue)];
            } else {
                int mod = (int) (hash % partitionLength);
                if (mod < 0) {
                    mod += partitionLength;
                }
                return segment[mod];
            }
        }

        public int partition(String key, int start, int end) {
            return partition(StringUtil.hash(key, start, end));
        }

    }

    public Partbylong(String cnt, String partLen) {
        init(cnt, partLen);
    }

    public void init(String partitionCount, String partitionLength) {
        this.count = toIntArray(partitionCount);
        this.length = toIntArray(partitionLength);
        partitionUtil = new PartitionUtil(count, length);
    }

    public Integer calculate(String columnValue) {
        try {
            if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
                return 0;
            }
            long key = Long.parseLong(columnValue);
            return calculate(key);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
        }
    }

    private Integer calculate(long key) {
        return partitionUtil.partition(key);
    }

    @Override
    public Object compute(Object[] args) {
        String str = (String) args[0];
        return calculate(str);
    }

    private int[] toIntArray(String string) {
        String[] strNumbers = SplitUtil.split(string, ',', true);
        int[] numbers = new int[strNumbers.length];
        for (int i = 0; i < strNumbers.length; ++i) {
            numbers[i] = Integer.parseInt(strNumbers[i]);
        }
        return numbers;
    }

}
