package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.partition.util.MySqlStringEscaperUtils;
import org.junit.Assert;
import org.junit.Test;

public class MySQLStringEscaperUtilsTest {

//    public static String escapeForMySQL(String input) {
//        if (input == null) {
//            return "NULL"; // 如果输入为 null，返回 SQL 中的 NULL
//        }
//
//        StringBuilder escaped = new StringBuilder();
//        for (int i = 0; i < input.length(); i++) {
//            char c = input.charAt(i);
//
//            // 根据 MySQL 转义规则处理每个字符
//            switch (c) {
//            case '\\': // 反斜杠
//                escaped.append("\\\\");
//                break;
//            case '\'': // 单引号
//                escaped.append("\\'");
//                break;
//            case '\n': // 换行符
//                escaped.append("\\n");
//                break;
//            case '\r': // 回车符
//                escaped.append("\\r");
//                break;
//            case '\t': // 制表符
//                escaped.append("\\t");
//                break;
//            default:
//                // 处理 ASCII 控制字符（ASCII 值小于 32）
//                if (c < 32) {
//                    // 转义为十六进制格式 \xHH
//                    escaped.append(String.format("\\x%02X", (int) c));
//                } else {
//                    // 普通字符直接追加
//                    escaped.append(c);
//                }
//                break;
//            }
//        }
//
//        return escaped.toString();
//    }

    @Test
    public void testEscapeForMySQL2() {

        {
            // case: '\\'
            String original = "\\";
            String escaped = MySqlStringEscaperUtils.escapeJavaStringForMySQL(original);
            String excepted = "\\\\";
            Assert.assertEquals(excepted, escaped);
            System.out.println("Original: " + original);
            System.out.println("Escaped: " + escaped);
            System.out.println("Excepted: " + excepted);
        }

        {
            // case: '\''
            String original = "'";
            String escaped = MySqlStringEscaperUtils.escapeJavaStringForMySQL(original);
            String excepted = "''";
            Assert.assertEquals(excepted, escaped);
            System.out.println("Original: " + original);
            System.out.println("Escaped: " + escaped);
            System.out.println("Excepted: " + excepted);
        }

        {
            // case: '\"'
            String original = "\"";
            String escaped = MySqlStringEscaperUtils.escapeJavaStringForMySQL(original);
            String excepted = "\"";
            Assert.assertEquals(excepted, escaped);
            System.out.println("Original: " + original);
            System.out.println("Escaped: " + escaped);
            System.out.println("Excepted: " + excepted);
        }

        {
            // case: '\n'
            String original = "\n";
            String escaped = MySqlStringEscaperUtils.escapeJavaStringForMySQL(original);
            String excepted = "\\n";
            Assert.assertEquals(excepted, escaped);
            System.out.println("Original: " + original);
            System.out.println("Escaped: " + escaped);
            System.out.println("Excepted: " + excepted);
        }

        {
            // case: '\r'
            String original = "\r";
            String escaped = MySqlStringEscaperUtils.escapeJavaStringForMySQL(original);
            String excepted = "\\r";
            Assert.assertEquals(excepted, escaped);
            System.out.println("Original: " + original);
            System.out.println("Escaped: " + escaped);
            System.out.println("Excepted: " + excepted);
        }

        {
            // case1: '\t'
            String original = "\t";
            String escaped = MySqlStringEscaperUtils.escapeJavaStringForMySQL(original);
            String excepted = "\\t";
            Assert.assertEquals(excepted, escaped);
            System.out.println("Original: " + original);
            System.out.println("Escaped: " + escaped);
            System.out.println("Excepted: " + excepted);
        }

    }


    @Test
    public void testEscapeForMySQL() {
        // 测试用例
        String original = "It's a \"test\" string with a newline\nand a tab\t.";
        String escaped = MySqlStringEscaperUtils.escapeJavaStringForMySQL(original);
        System.out.println("Original: " + original);
        System.out.println("Escaped: " + escaped);

        // 包含控制字符的测试
        String controlChars = "Control chars:\u0001\u0002\u0003";
        String escapedControlChars = MySqlStringEscaperUtils.escapeJavaStringForMySQL(controlChars);
        System.out.println("Original: " + controlChars);
        System.out.println("Escaped: " + escapedControlChars);

        // 测试 null 输入
        String nullInput = null;
        String escapedNull = MySqlStringEscaperUtils.escapeJavaStringForMySQL(nullInput);
        System.out.println("Original: null");
        System.out.println("Escaped: " + escapedNull);
    }
}
