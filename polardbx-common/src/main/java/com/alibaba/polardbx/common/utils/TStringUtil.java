package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * TDDL专用的字符处理便捷类，集成了apache common StringUtils类，方便字符串处理
 *
 * @author linxuan
 * @author jianghang 2013-10-24 下午4:02:24
 * @since 5.0.0
 */
public class TStringUtil extends StringUtils {

    private final static long[] pow10 = {
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
        10000000000L, 100000000000L, 1000000000000L, 10000000000000L, 100000000000000L, 1000000000000000L,
        10000000000000000L, 100000000000000000L, 1000000000000000000L};

    protected static final char[] HEX_CHAR = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
        'e', 'f'};

    private static final String CHAR_LOWER = "abcdefghijklmnopqrstuvwxyz";
    private static final String CHAR_UPPER = CHAR_LOWER.toUpperCase();
    private static final String DIGIT = "0123456789";
    private static final String DEFAULT_CHAR_SET = CHAR_LOWER + CHAR_UPPER + DIGIT;

    /**
     * 获得第一个start，end之间的字串， 不包括start，end本身。返回值已做了trim
     *
     * <pre>
     * TStringUtil.getBetween("wx[ b ]yz", "[", "]") = "b"
     * TStringUtil.getBetween(null, *, *)          = null
     * TStringUtil.getBetween(*, null, *)          = null
     * TStringUtil.getBetween(*, *, null)          = null
     * TStringUtil.getBetween("", "", "")          = ""
     * TStringUtil.getBetween("", "", "]")         = null
     * TStringUtil.getBetween("", "[", "]")        = null
     * TStringUtil.getBetween("yabcz", "", "")     = ""
     * TStringUtil.getBetween("yabcz", "y", "z")   = "abc"
     * TStringUtil.getBetween("yabczyabcz", "y", "z")   = "abc"
     * </pre>
     */
    public static String getBetween(String sql, String start, String end) {
        if (sql == null || start == null || end == null) {
            return null;
        }

        int index0 = sql.indexOf(start);
        if (index0 == -1) {
            return null;
        }
        int index1 = sql.indexOf(end, index0);
        if (index1 == -1) {
            return null;
        }
        return sql.substring(index0 + start.length(), index1).trim();
    }

    /**
     * 去除第一个start,end之间的字符串，包括start,end本身
     *
     * <pre>
     * TStringUtil.removeBetween(&quot;abc[xxx]bc&quot;, &quot;[&quot;, &quot;]&quot;) = &quot;abc bc&quot;
     * </pre>
     */
    public static String removeBetween(String sql, String start, String end) {
        if (sql == null) {
            return null;
        }

        if (start == null || end == null) {
            return sql;
        }

        int index0 = sql.indexOf(start);
        if (index0 == -1) {
            return sql;
        }
        int index1 = sql.indexOf(end, index0);
        if (index1 == -1) {
            return sql;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sql.substring(0, index0));
        sb.append(" ");
        sb.append(sql.substring(index1 + end.length()));
        return sb.toString();
    }

    /**
     * 只做一次切分
     *
     * <pre>
     * TStringUtil.twoPartSplit("abc:bc:bc", ":") = ["abc","bc:bc"]
     * TStringUtil.twoPartSplit(null, *)          = [null]
     * TStringUtil.twoPartSplit("abc:bc", null)   = ["abc:bc"]
     * TStringUtil.twoPartSplit("abc:bc", ";")    = ["abc:bc:bc"]
     * </pre>
     */
    public static String[] twoPartSplit(String str, String splitor) {
        if (str != null && splitor != null) {
            int index = str.indexOf(splitor);
            if (index != -1) {
                String first = str.substring(0, index);
                String sec = str.substring(index + splitor.length());
                return new String[] {first, sec};
            } else {
                return new String[] {str};
            }
        } else {
            return new String[] {str};
        }
    }

    /**
     * 递归调用twoPartSplit进行切分
     *
     * <pre>
     * TStringUtil.twoPartSplit("abc:bc:bc", ":") = ["abc","bc","bc"]
     * TStringUtil.twoPartSplit(null, *)          = [null]
     * TStringUtil.twoPartSplit("abc:bc", null)   = ["abc:bc"]
     * TStringUtil.twoPartSplit("abc:bc", ";")    = ["abc:bc"]
     * </pre>
     */
    public static List<String> recursiveSplit(String str, String splitor) {
        List<String> re = new ArrayList<String>();
        String[] strs = twoPartSplit(str, splitor);
        if (strs.length == 2) {
            re.add(strs[0]);
            re.addAll(recursiveSplit(strs[1], splitor));
        } else {
            re.add(strs[0]);
        }
        return re;
    }

    /**
     * 将所有/t/s/n等空白符全部替换为空格，并且去除多余空白
     *
     * <pre>
     * TStringUtil.fillTabWithSpace(&quot;abc   bc  &quot;) = &quot;abc bc &quot;
     * </pre>
     */
    public static String fillTabWithSpace(String str) {
        if (str == null) {
            return null;
        }

        str = str.trim();
        int sz = str.length();
        StringBuilder buffer = new StringBuilder(sz);

        int index = 0, index0 = -1, index1 = -1;
        for (int i = 0; i < sz; i++) {
            char c = str.charAt(i);
            if (!Character.isWhitespace(c)) {
                if (index0 != -1) {
                    // if (!(index0 == index1 && str.charAt(i - 1) == ' ')) {
                    if (index0 != index1 || str.charAt(i - 1) != ' ') {
                        buffer.append(str.substring(index, index0)).append(" ");
                        index = index1 + 1;
                    }
                }
                index0 = index1 = -1;
            } else {
                if (index0 == -1) {
                    index0 = index1 = i; // 第一个空白
                } else {
                    index1 = i;
                }
            }
        }

        buffer.append(str.substring(index));
        return buffer.toString();
    }

    /**
     * Determines whether or not the sting 'searchIn' contains the string
     * 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn the string to search in
     * @param searchFor the string to search for
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */
    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor) {
        return startsWithIgnoreCaseAndWs(searchIn, searchFor, 0);
    }

    /**
     * Determines whether or not the sting 'searchIn' contains the string
     * 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn the string to search in
     * @param searchFor the string to search for
     * @param beginPos where to start searching
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */
    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos) {
        if (searchIn == null) {
            return searchFor == null;
        }

        int inLength = searchIn.length();

        for (; beginPos < inLength; beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return startsWithIgnoreCase(searchIn, beginPos, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', dis-regarding case starting at 'startAt' Shorthand for a
     * String.regionMatch(...)
     *
     * @param searchIn the string to search in
     * @param startAt the position to start at
     * @param searchFor the string to search for
     * @return whether searchIn starts with searchFor, ignoring case
     */
    public static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    /**
     * Returns the given string, with comments removed
     *
     * @param src the source string
     * @param stringOpens characters which delimit the "open" of a string
     * @param stringCloses characters which delimit the "close" of a string, in
     * counterpart order to <code>stringOpens</code>
     * @param slashStarComments strip slash-star type "C" style comments
     * @param slashSlashComments strip slash-slash C++ style comments to
     * end-of-line
     * @param hashComments strip #-style comments to end-of-line
     * @param dashDashComments strip "--" style comments to end-of-line
     * @return the input string with all comment-delimited data removed
     */
    public static String stripComments(String src, String stringOpens, String stringCloses, boolean slashStarComments,
                                       boolean slashSlashComments, boolean hashComments, boolean dashDashComments) {
        if (src == null) {
            return null;
        }

        StringBuffer buf = new StringBuffer(src.length());

        // It's just more natural to deal with this as a stream
        // when parsing..This code is currently only called when
        // parsing the kind of metadata that developers are strongly
        // recommended to cache anyways, so we're not worried
        // about the _1_ extra object allocation if it cleans
        // up the code

        StringReader sourceReader = new StringReader(src);

        int contextMarker = Character.MIN_VALUE;
        boolean escaped = false;
        int markerTypeFound = -1;

        int ind = 0;

        int currentChar = 0;

        try {
            while ((currentChar = sourceReader.read()) != -1) {
                if (currentChar == '\\') {
                    escaped = !escaped;
                } else if (markerTypeFound != -1 && currentChar == stringCloses.charAt(markerTypeFound) && !escaped) {
                    contextMarker = Character.MIN_VALUE;
                    markerTypeFound = -1;
                } else if ((ind = stringOpens.indexOf(currentChar)) != -1 && !escaped
                    && contextMarker == Character.MIN_VALUE) {
                    markerTypeFound = ind;
                    contextMarker = currentChar;
                }

                if (contextMarker == Character.MIN_VALUE && currentChar == '/'
                    && (slashSlashComments || slashStarComments)) {
                    currentChar = sourceReader.read();
                    if (currentChar == '*' && slashStarComments) {
                        int prevChar = 0;
                        while ((currentChar = sourceReader.read()) != '/' || prevChar != '*') {
                            if (currentChar == '\r') {

                                currentChar = sourceReader.read();
                                if (currentChar == '\n') {
                                    currentChar = sourceReader.read();
                                }
                            } else {
                                if (currentChar == '\n') {

                                    currentChar = sourceReader.read();
                                }
                            }
                            if (currentChar < 0) {
                                break;
                            }
                            prevChar = currentChar;
                        }
                        continue;
                    } else if (currentChar == '/' && slashSlashComments) {
                        while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                            ;
                        }
                    }
                } else if (contextMarker == Character.MIN_VALUE && currentChar == '#' && hashComments) {
                    // Slurp up everything until the newline
                    while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                        ;
                    }
                } else if (contextMarker == Character.MIN_VALUE && currentChar == '-' && dashDashComments) {
                    currentChar = sourceReader.read();

                    if (currentChar == -1 || currentChar != '-') {
                        buf.append('-');

                        if (currentChar != -1) {
                            buf.append(currentChar);
                        }

                        continue;
                    }

                    // Slurp up everything until the newline

                    while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                        ;
                    }
                }

                if (currentChar != -1) {
                    buf.append((char) currentChar);
                }
            }
        } catch (IOException ioEx) {
            // we'll never see this from a StringReader
        }

        return buf.toString();
    }

    public static String removeBetweenWithSplitorNotExistNull(String sql, String start, String end) {
        int index0 = sql.indexOf(start);
        if (index0 == -1) {
            return null;
        }
        int index1 = sql.indexOf(end, index0);
        if (index1 == -1) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sql.substring(0, index0));
        sb.append(" ");
        sb.append(sql.substring(index1 + end.length()));
        return sb.toString();
    }

    /**
     * 简单地检查是否是逻辑表与具体子表的关系。子表名满足父表名+"_数字";
     */
    public static boolean isTableFatherAndSon(String fatherTable, String sonTable) {
        if (fatherTable == null || fatherTable.trim().isEmpty() || sonTable == null || sonTable.trim().isEmpty()) {
            return false;
        }
        if (!sonTable.startsWith(fatherTable) || fatherTable.length() + 2 > sonTable.length()) {
            return false;
        }
        String suffix = sonTable.substring(fatherTable.length());
        if (suffix.matches("_[\\d]+")) {
            return true;
        }
        return false;

    }

    public static int compareTo(String str1, String str2) {
        if (str1 == null && str2 == null) {
            return 0;
        }

        if (str1 == null) {
            return -str2.compareTo(str1);
        }
        return str1.compareTo(str2);
    }

    public static String replaceWithIgnoreCase(String str, String searchStr, String replaceStr) {
        int index = indexOfIgnoreCase(str, searchStr);
        if (index < 0) {
            return str;
        } else {
            if (index > 0) {
                return str.substring(0, index) + replaceStr + str.substring(index + searchStr.length());
            } else {
                return replaceStr + str.substring(searchStr.length());
            }
        }
    }

    private static Method toPlainStringMethod;

    static {

        try {
            toPlainStringMethod = BigDecimal.class.getMethod("toPlainString", new Class[0]);
        } catch (NoSuchMethodException nsme) {
            // that's okay, we fallback to .toString()
        }
    }

    public static final String fixDecimalExponent(String dString) {
        int ePos = dString.indexOf("E"); //$NON-NLS-1$

        if (ePos == -1) {
            ePos = dString.indexOf("e"); //$NON-NLS-1$
        }

        if (ePos != -1) {
            if (dString.length() > (ePos + 1)) {
                char maybeMinusChar = dString.charAt(ePos + 1);

                if (maybeMinusChar != '-' && maybeMinusChar != '+') {
                    StringBuffer buf = new StringBuffer(dString.length() + 1);
                    buf.append(dString.substring(0, ePos + 1));
                    buf.append('+');
                    buf.append(dString.substring(ePos + 1, dString.length()));
                    dString = buf.toString();
                }
            }
        }

        return dString;
    }

    public static String consistentToString(BigDecimal decimal) {
        if (decimal == null) {
            return null;
        }

        if (toPlainStringMethod != null) {
            try {
                return (String) toPlainStringMethod.invoke(decimal, (Object[]) null);
            } catch (InvocationTargetException invokeEx) {
                // that's okay, we fall-through to decimal.toString()
            } catch (IllegalAccessException accessEx) {
                // that's okay, we fall-through to decimal.toString()
            }
        }

        return decimal.toString();
    }

    public static String formatNanos(int nanos, boolean serverSupportsFracSecs) {
        if (!serverSupportsFracSecs || nanos == 0) {
            return "0";
        }

        boolean usingMicros = true;
        if (usingMicros) {
            nanos /= 1000;
        }

        final int digitCount = usingMicros ? 6 : 9;
        String nanosString = Integer.toString(nanos);
        final String zeroPadding = usingMicros ? "000000" : "000000000";

        nanosString = zeroPadding.substring(0, (digitCount - nanosString.length())) + nanosString;
        int pos = digitCount - 1; // the end, we're padded to the end by the
        // code above
        while (nanosString.charAt(pos) == '0') {
            pos--;
        }

        nanosString = nanosString.substring(0, pos + 1);
        return nanosString;
    }

    /**
     * @param bit 补齐后的长度
     * @param table 数值
     * @return 返回前面补0达到bit长度的字符串。如果table长度大于bit，则返回table的原始值
     */
    public static String placeHolder(int bit, long table) {
        if (bit > 18) {
            throw new IllegalArgumentException("截取的位数不能大于18位");
        }
        if (table == 0) {
            // bugfix 被0除
            return String.valueOf(pow10[bit]).substring(1);
        }
        if (table >= pow10[bit - 1]) {
            // 当数值的width >= 要求的补齐位数时，应该直接返回原始数值
            return String.valueOf(table);
        }
        long max = pow10[bit];
        long placedNumber = max + table;
        return String.valueOf(placedNumber).substring(1);
    }

    public static boolean isEmpty(StringBuilder str) {
        return str == null || str.length() == 0;
    }

    public static boolean isEscapeNeededForString(String x, int stringLength) {
        boolean needsHexEscape = false;
        for (int i = 0; i < stringLength; ++i) {
            char c = x.charAt(i);
            switch (c) {
            case 0: /* Must be escaped for 'mysql' */
                needsHexEscape = true;
                break;
            case '\n': /* Must be escaped for logs */
                needsHexEscape = true;
                break;
            case '\r':
                needsHexEscape = true;
                break;
            case '\\':
                needsHexEscape = true;
                break;
            case '\'':
                needsHexEscape = true;
                break;
            case '"': /* Better safe than sorry */
                needsHexEscape = true;
                break;
            case '\032': /* This gives problems on Win32 */
                needsHexEscape = true;
                break;
            }

            if (needsHexEscape) {
                break; // no need to scan more
            }
        }
        return needsHexEscape;
    }

    /**
     * Wrap the string with back-quote.
     */
    public static String backQuote(String x) {
        return "`" + x + "`";
    }

    public static String quoteString(String x) {
        String parameterAsString = null;
        boolean usingAnsiMode = false;
        int stringLength = x.length();
        StringBuffer buf = new StringBuffer((int) (x.length() * 1.1));
        buf.append('\'');
        if (isEscapeNeededForString(x, stringLength)) {
            //
            // Note: buf.append(char) is _faster_ than
            // appending in blocks, because the block
            // append requires a System.arraycopy()....
            // go figure...
            //
            for (int i = 0; i < stringLength; ++i) {
                char c = x.charAt(i);
                switch (c) {
                case 0: /* Must be escaped for 'mysql' */
                    buf.append('\\');
                    buf.append('0');
                    break;
                case '\n': /* Must be escaped for logs */
                    buf.append('\\');
                    buf.append('n');
                    break;
                case '\r':
                    buf.append('\\');
                    buf.append('r');
                    break;
                case '\\':
                    buf.append('\\');
                    buf.append('\\');
                    break;
                case '\'':
                    buf.append('\\');
                    buf.append('\'');
                    break;
                case '"': /* Better safe than sorry */
                    if (usingAnsiMode) {
                        buf.append('\\');
                    }
                    buf.append('"');
                    break;
                case '\032': /* This gives problems on Win32 */
                    buf.append('\\');
                    buf.append('Z');
                    break;
                case '\u00a5':
                case '\u20a9':
                    return null; // 处理不了，返回null值，回退到prepare结构
                default:
                    buf.append(c);
                }
            }
        } else {
            buf.append(x);
        }

        buf.append('\'');
        parameterAsString = buf.toString();
        return parameterAsString;
    }

    public static String bytesToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            if ((0xff & b) < 0x10) {
                sb.append('0').append(Integer.toHexString((0xFF & b)));
            } else {
                sb.append(Integer.toHexString(0xFF & b));
            }
        }
        return sb.toString();
    }

    /**
     * <p>
     * Checks if String contains a search String irrespective of case, handling
     * <code>null</code>. Case-insensitivity is defined as by
     * {@link String#equalsIgnoreCase(String)}.
     * <p>
     * A <code>null</code> String will return <code>false</code>.
     * </p>
     *
     * <pre>
     * StringUtils.contains(null, *) = false
     * StringUtils.contains(*, null) = false
     * StringUtils.contains("", "") = true
     * StringUtils.contains("abc", "") = true
     * StringUtils.contains("abc", "a") = true
     * StringUtils.contains("abc", "z") = false
     * StringUtils.contains("abc", "A") = true
     * StringUtils.contains("abc", "Z") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @param searchStr the String to find, may be null
     * @return true if the String contains the search String irrespective of
     * case or false if not or <code>null</code> string input
     */
    public static boolean containsIgnoreCase(String str, String searchStr) {
        if (str == null || searchStr == null) {
            return false;
        }
        int len = searchStr.length();
        if (len == 0) {
            return true;
        }
        char first = Character.toLowerCase(searchStr.charAt(0));
        int max = str.length() - len;
        for (int i = 0; i <= max; i++) {
            char ch = Character.toLowerCase(str.charAt(i));
            /* Look for first character. */
            if (ch != first) {
                continue;
            }
            /* Found first character, now look at the rest */
            if (str.regionMatches(true, i + 1, searchStr, 1, len - 1)) {
                return true;
            }
        }
        return false;
    }

    /**
     * <p>
     * Checks if the String contains any character in the given set of
     * characters.
     * </p>
     * <p>
     * A <code>null</code> String will return <code>false</code>. A
     * <code>null</code> or zero length search array will return
     * <code>false</code>.
     * </p>
     *
     * <pre>
     * StringUtils.containsAny(null, *)                = false
     * StringUtils.containsAny("", *)                  = false
     * StringUtils.containsAny(*, null)                = false
     * StringUtils.containsAny(*, [])                  = false
     * StringUtils.containsAny("zzabyycdxx",['z','a']) = true
     * StringUtils.containsAny("zzabyycdxx",['b','y']) = true
     * StringUtils.containsAny("aba", ['z'])           = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @param searchChars the chars to search for, may be null
     * @return the <code>true</code> if any of the chars are found,
     * <code>false</code> if no match or null input
     */
    public static boolean containsAny(String str, char[] searchChars) {
        if (isEmpty(str) || searchChars == null || searchChars.length == 0) {
            return false;
        }
        int csLength = str.length();
        int searchLength = searchChars.length;
        int csLast = csLength - 1;
        int searchLast = searchLength - 1;
        for (int i = 0; i < csLength; i++) {
            char ch = str.charAt(i);
            for (int j = 0; j < searchLength; j++) {
                if (searchChars[j] == ch) {
                    if (isHighSurrogate(ch)) {
                        if (j == searchLast) {
                            // missing low surrogate, fine, like
                            // String.indexOf(String)
                            return true;
                        }
                        if (i < csLast && searchChars[j + 1] == str.charAt(i + 1)) {
                            return true;
                        }
                    } else {
                        // ch is in the Basic Multilingual Plane
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean isHighSurrogate(char ch) {
        return ('\uD800' <= ch && '\uDBFF' >= ch);
    }

    /**
     * 下划线格式转驼峰格式
     */
    public static String convertToHumpStr(String str) {
        StringBuilder sb = new StringBuilder();
        boolean match = false;
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (match && ch >= 97 && ch <= 122) {
                ch -= 32;
            }
            if (ch != '_') {
                match = false;
                sb.append(ch);
            } else {
                match = true;
            }
        }

        return sb.toString();
    }

    /**
     * 驼峰转下划线格式
     */
    public static String convertToSnakeStr(String str) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (Character.getType(ch) == Character.UPPERCASE_LETTER) {
                sb.append("_" + Character.toLowerCase(ch));
            } else {
                sb.append(ch);
            }
        }

        return sb.toString();
    }

    /**
     * <p>Checks whether the given String is a parsable number.</p>
     *
     * <p>Parsable numbers include those Strings understood by {@link Integer#parseInt(String)},
     * {@link Long#parseLong(String)}, {@link Float#parseFloat(String)} or
     * {@link Double#parseDouble(String)}. This method can be used instead of catching {@link java.text.ParseException}
     * when calling one of those methods.</p>
     *
     * <p>Hexadecimal and scientific notations are <strong>not</strong> considered parsable.
     * See org.apache.commons.lang3.math.NumberUtils.isNumber(String) on those cases.</p>
     *
     * <p>{@code Null} and empty String will return <code>false</code>.</p>
     *
     * @param str the String to check.
     * @return {@code true} if the string is a parsable number.
     * @since 3.4
     */
    public static boolean isParsableNumber(String str) {
        if (StringUtils.endsWith(str, ".")) {
            return false;
        }
        if (StringUtils.startsWith(str, "-")) {
            return NumberUtils.isDigits(StringUtils.replaceOnce(str.substring(1), ".", StringUtils.EMPTY));
        } else {
            return NumberUtils.isDigits(StringUtils.replaceOnce(str, ".", StringUtils.EMPTY));
        }
    }

    /**
     * 查找字符串末尾的连续数字, 返回第一个数字的位置 (正则表达式: "\d+$")
     */
    public static int lastDigitOf(String str) {
        int n = str.length() - 1;

        if (!Character.isDigit(str.charAt(n))) {
            return -1;
        }
        for (int i = n - 1; i >= 0; i--) {
            if (!Character.isDigit(str.charAt(i))) {
                return i + 1;
            }
        }
        return 0;
    }

    /**
     * 查找字符串中需要转义的关键字符, 并且在关键字符前加上前缀。
     *
     * @param findChar - 关键字符, 例如 '\''
     * @param leadChar - 前缀字符, 例如 '\\'
     */
    public static String escape(String str, char findChar, char leadChar) {
        int find = str.indexOf(findChar);
        if (find < 0) {
            return str;
        }
        StringBuilder builder = new StringBuilder(str.length() + 8);
        int index = 0;
        do {
            builder.append(str.substring(index, find));
            builder.append(leadChar);
            index = find;
            find = str.indexOf(findChar, find + 1);
        } while (find >= 0);

        builder.append(str.substring(index));
        return builder.toString();
    }

    public static String objToString(Object obj) {
        if (obj == null) {
            return null;
        }
        return obj.toString();
    }

    /**
     * 授权和权限校验有多处需用到 toUpperCase, 故整合在一处，以便检索
     */
    public static String normalizePriv(String str) {
        return StringUtils.isBlank(str) ? str : str.toUpperCase();
    }

    public static String addBacktick(String str) {
        if (StringUtils.isNotBlank(str) && !str.startsWith("`")) {
            str = "`" + str + "`";
        }
        return str;
    }

    /**
     * used for result set encoding
     */
    public static String javaEncoding(String encoding) {
        if (encoding.equalsIgnoreCase("utf8mb4")) {
            return "utf8";
        } else if (encoding.equalsIgnoreCase("binary")) {
            if (DynamicConfig.getInstance().isCompatibleCharsetVariables()) {
                // compatible with MySQL's behavior
                // resultSet encoding is binary, which means no conversion
                return "utf8";
            } else {
                // compatible with the old behavior
                return "iso_8859_1";
            }
        }
        return encoding;
    }

    public static String getUnescapedString(String string) {
        return getUnescapedString(string, false);
    }

    public static String getUnescapedString(String string, boolean toUppercase) {
        StringBuilder sb = new StringBuilder();
        char[] chars = string.toCharArray();
        for (int i = 0; i < chars.length; ++i) {
            char c = chars[i];
            if (c == '\\') {
                switch (c = chars[++i]) {
                case '0':
                    sb.append('\0');
                    break;
                case 'b':
                    sb.append('\b');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'Z':
                    sb.append((char) 26);
                    break;
                case '\'':
                    sb.append('\'');
                    break;
                case '\\':
                    sb.append('\\');
                    break;
                case '\"':
                    sb.append("\"");
                    break;
                default:
                    sb.append(c);
                }
            } else if (c == '\'') {
                ++i;
                sb.append('\'');
            } else {
                if (toUppercase && c >= 'a' && c <= 'z') {
                    c -= 32;
                }
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Convert int val to fixed length hex string
     */
    public static String int2FixedLenHexStr(int intVal) {
        // convert int to bytes
        byte[] hashValBytes = intToByteArray(intVal);
        // convert bytes to hex str
        return bytesToHex(hashValBytes);
    }

    protected static String bytesToHex(byte[] bytes) {
        // 一个byte为8位，可用两个十六进制位标识
        char[] buf = new char[bytes.length * 2];
        int a = 0;
        int index = 0;
        for (byte b : bytes) { // 使用除与取余进行转换
            if (b < 0) {
                a = 256 + b;
            } else {
                a = b;
            }
            buf[index++] = HEX_CHAR[a / 16];
            buf[index++] = HEX_CHAR[a % 16];
        }
        return new String(buf);
    }

    protected static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        result[0] = (byte) ((i >> 24) & 0xFF);
        result[1] = (byte) ((i >> 16) & 0xFF);
        result[2] = (byte) ((i >> 8) & 0xFF);
        result[3] = (byte) (i & 0xFF);
        return result;
    }

    public static int hex2Int(String hexString) {
        int value = 0;
        for (int i = 0; i < hexString.length(); i++) {
            char ch = hexString.charAt(i);
            if (ch >= '0' && ch <= '9') {
                value <<= 4;
                value |= ch - '0';
                continue;
            }
            if (ch >= 'a' && ch <= 'f') {
                value <<= 4;
                value |= ch - 'a' + 10;
                continue;
            }
        }
        return value;
    }

    public static String[] truncate(String[] strings, int maxStringLength) {
        if (strings == null) {
            return null;
        }
        boolean needTruncate = false;
        for (String string : strings) {
            if (string != null && string.length() > maxStringLength) {
                needTruncate = true;
                break;
            }
        }

        if (!needTruncate) {
            return strings;
        }

        String[] strings1 = new String[strings.length];
        for (int i = 0; i < strings.length; i++) {
            if (strings[i] != null && strings[i].length() > maxStringLength) {
                strings1[i] = strings[i].substring(0, maxStringLength);
            } else {
                strings1[i] = strings[i];
            }

        }
        return strings1;
    }

    public static String concatTableName(String schema, String table) {
        return backQuote(schema) + "." + backQuote(table);
    }

    public static String quote(String str) {
        return str != null ? "'" + str + "'" : null;
    }

    public static String createRandomString(int length, String charSet){
        if (charSet == null || charSet.isEmpty()) {
            charSet = DEFAULT_CHAR_SET;
        }
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = ThreadLocalRandom.current().nextInt(charSet.length());
            sb.append(charSet.charAt(randomIndex));
        }
        return sb.toString();
    }

}
