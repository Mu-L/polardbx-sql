/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.optimizer.core.function.calc.dble;

import com.alibaba.polardbx.druid.util.StringUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public final class PartitionByUnitMurmurHash extends DblePartitionAlgorithm implements RuleAlgorithm {

    public static class MurmurHash {

        // MurMurHash
        public static final String TABLE_COLUMN_SEPARATOR = ".";

        private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
        private static final Random RANDOM = new Random();
        private static final char[] CHARS = {
            '1', '2', '3', '4', '5', '6', '7',
            '8', '9', '0', 'q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o', 'p',
            'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v',
            'b', 'n', 'm', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
            'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Z', 'X', 'C', 'V',
            'B', 'N', 'M'};

        public MurmurHash() {
        }

        public static long murmurHash(String key) {

            ByteBuffer buf = ByteBuffer.wrap(key.getBytes());
            int seed = 0x1234ABCD;

            ByteOrder byteOrder = buf.order();
            buf.order(ByteOrder.LITTLE_ENDIAN);

            long m = 0xc6a4a7935bd1e995L;
            int r = 47;

            long h = seed ^ (buf.remaining() * m);

            long k;
            while (buf.remaining() >= 8) {
                k = buf.getLong();

                k *= m;
                k ^= k >>> r;
                k *= m;

                h ^= k;
                h *= m;
            }

            if (buf.remaining() > 0) {
                ByteBuffer finish = ByteBuffer.allocate(8).order(
                    ByteOrder.LITTLE_ENDIAN);
                // for big-endian version, do this first:
                // finish.position(8-buf.remaining());
                finish.put(buf).rewind();
                h ^= finish.getLong();
                h *= m;
            }

            h ^= h >>> r;
            h *= m;
            h ^= h >>> r;

            buf.order(byteOrder);
            return Math.abs(h);
        }

        public static boolean isDigit(String strNum) {
            return strNum.matches("[0-9]+");
        }
    }

    public PartitionByUnitMurmurHash() {
    }

    private static final long serialVersionUID = 3777423001153345948L;
    private static final String CNO_DEFAULT_LENGTH = "15";
    protected String partitionCount;
    protected String isDigitCheck;
    protected String shardingValueMaxLength;
    protected String leftPaddingChar;
    private long maxHashDivisor;
    public final static String simpleString = "0123456789";
    public final static String CHECK_CLOSE = "0";
    public final static String CHECK_OPEN = "1";
    public final static String DEFAULT_PADDING_CHAR = "NO";
    protected int hashCode = -1;

    public void setPartitionCount(String partitionCount) {
        this.partitionCount = partitionCount;
        propertiesMap.put("partitionCount", partitionCount);
    }

    public String getIsDigitCheck() {
        if ("".equals(isDigitCheck) || isDigitCheck == null) {
            isDigitCheck = CHECK_OPEN;
        }
        return isDigitCheck;
    }

    public boolean isDigitCheck() {
        // default = 1(open)
        if (CHECK_CLOSE.equals(getIsDigitCheck())) {
            return false;
        } else {
            return true;
        }
    }

    public void setIsDigitCheck(String isDigitCheck) {
        this.isDigitCheck = isDigitCheck;
        propertiesMap.put("isDigitCheck", CHECK_OPEN);
    }

    // padding char into column on left
    public void setLeftPaddingChar(String leftPaddingChar) {
        this.leftPaddingChar = leftPaddingChar;
        propertiesMap.put("leftPaddingChar", this.leftPaddingChar);
    }

    public String getLeftPaddingChar() {
        if ("".equals(leftPaddingChar) || leftPaddingChar == null) {
            this.leftPaddingChar = DEFAULT_PADDING_CHAR;
        }
        return this.leftPaddingChar;
    }

    public String getShardingValueMaxLength() {
        if ("".equals(this.shardingValueMaxLength) || this.shardingValueMaxLength == null) {
            this.shardingValueMaxLength = CNO_DEFAULT_LENGTH;
        }
        return this.shardingValueMaxLength;
    }

    public void setShardingValueMaxLength(String shardingValueMaxLength) {
        this.shardingValueMaxLength = shardingValueMaxLength;
        propertiesMap.put("shardingValueMaxLength", this.shardingValueMaxLength);
    }

    @Override
    public void init() {
        Double partitionLog = Math.log(Double.valueOf(this.partitionCount)) / Math.log(2);
        this.maxHashDivisor = Math.round(Math.pow(2, 63 - partitionLog));
        initHashCode();
    }

    /**
     * return sharding nodes's id
     * columnValue is column's value
     *
     * @return never null
     */
    @Override
    public Integer calculate(String columnValue) {
        try {
            columnValue = checkColumn(columnValue);
            long hash = MurmurHash.murmurHash(columnValue);
            return (int) mapping(hash, this.maxHashDivisor) - 1;
        } catch (Exception e) {
            throw new IllegalArgumentException("columnValue:'" + columnValue + "', Please check routeKey is legal. " + e.getMessage(), e);
        }
    }

    /**
     * route column check
     */
    public String checkColumn(String columnValue) throws NumberFormatException {
        // customer number
        if ("".equals(columnValue) || columnValue == null) {
            // no customer number,just check null or "";
            try {
                throw new IllegalArgumentException("routeField is null or \"\" !");
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        } else if (isDigitCheck()) {
            int columnLength = Integer.valueOf(getShardingValueMaxLength());
            // is digit?
            if (!MurmurHash.isDigit(columnValue.trim())) {
                try {
                    throw new IllegalArgumentException("routeField is not digit !");
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
            // length check;default 1;stop padding which value is 0
            if (columnLength > 0) {
                columnValue = addZeroForKey(columnValue.trim(), columnLength);
            }
        } else {
            // no left padding char
            if ("NO".equals(getLeftPaddingChar())) {
                // origin keyValue
                return columnValue;
            }
            // padding left char by "leftPaddingChar" with shardingValueMaxLength
            columnValue = addCharForKey(columnValue.trim(), Integer.valueOf(getShardingValueMaxLength()));
        }
        return columnValue;
    }

    /**
     * return the index of node
     * return an empty arrayNumberFormat means router to all node
     * return null if no node matches
     */
    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        return new Integer[0];
    }

    @Override
    public void selfCheck() {
        if (this.partitionCount == null || this.partitionCount.length() < 1
            || Integer.valueOf(this.partitionCount) == 0) {
            throw new RuntimeException("set PartitionCount is null or is zero");
        }
        if ((Integer.valueOf(this.partitionCount) & (Integer.valueOf(this.partitionCount) - 1)) != 0) {
            throw new RuntimeException("PartitionCount is not Power of 2  or PartitionCount is zero.");
        }
    }

    /*
     * @Describe:¼ÆËã·ÖÆ¬ºÅ
     */
    public static long mapping(long hashValue, long maxHashValue) {
        if (hashValue == 0L) {
            return 1L;
        }
        return Math.floorDiv(hashValue - 1, maxHashValue) + 1;
    }

    public String addZeroForKey(String str, int strLength) {
        int strLen = str.length();
        if (strLen > strLength) {
            throw new IllegalArgumentException("routeField is too long, over " + strLength);
        }
        StringBuffer sb = new StringBuffer();
        while (strLen < strLength) {
            sb.append("0");
            strLen++;
        }
        return sb.toString().concat(str);
    }

    /**
     * @param str, strLength
     */
    public String addCharForKey(String str, int strLength) {
        int strLen = str.length();
        if (strLen > strLength) {
            try {
                throw new IllegalArgumentException("routeField is too long, over " + strLength);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
        StringBuffer sb = new StringBuffer();
        while (strLen < strLength) {
            sb.append(getLeftPaddingChar());
            strLen++;
        }
        return sb.toString().concat(str);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private void initHashCode() {
        if (this.partitionCount != null) {
            hashCode ^= this.partitionCount.hashCode();
        }
        if (this.isDigitCheck != null) {
            hashCode ^= this.isDigitCheck.hashCode();
        }
        if (this.shardingValueMaxLength != null) {
            hashCode ^= this.shardingValueMaxLength.hashCode();
        }
        if (this.leftPaddingChar != null) {
            hashCode ^= this.leftPaddingChar.hashCode();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionByUnitMurmurHash other = (PartitionByUnitMurmurHash) o;
        if (!StringUtils.equals(other.partitionCount, this.partitionCount)) {
            return false;
        }
        if (!StringUtils.equals(other.isDigitCheck, this.isDigitCheck)) {
            return false;
        }
        if (!StringUtils.equals(other.shardingValueMaxLength, this.shardingValueMaxLength)) {
            return false;
        }
        if (!StringUtils.equals(other.leftPaddingChar, this.leftPaddingChar)) {
            return false;
        }
        return true;
    }
}
