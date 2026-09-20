package com.alibaba.polardbx.optimizer.partition.util;

/**
 * @author chenghui.lch
 */
public class MySqlStringEscaperUtils {
    public static String escapeJavaStringForMySQL(String input) {
        if (input == null) {
            return "null";
        }

        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            switch (c) {
            case '\\':
                // \ (raw java char)
                // change into \\ (raw mysql varchar)
                // converted from "\\\\"  (raw java string of raw mysql varchar)
                escaped.append("\\\\");
                break;
            case '\'':
                // ' (raw java char)
                // change into '' (raw mysql varchar)
                // converted from "''"  (raw java string of raw mysql varchar)
                escaped.append("''");
                break;
            case '\"':
                // " (raw java char)
                // change into " (raw mysql varchar)
                // converted from "\""  (raw java string of raw mysql varchar)
                escaped.append("\"");
                break;
            case '\n':
                // \n (raw java char)
                // change into \n (raw mysql varchar)
                // converted from "\\n"  (raw java string of raw mysql varchar)
                escaped.append("\\n");
                break;
            case '\r':
                // \r (raw java char)
                // change into \r (raw mysql varchar)
                // converted from "\\r"  (raw java string of raw mysql varchar)
                escaped.append("\\r");
                break;
            case '\t':
                // \t (raw java char)
                // change into \t (raw mysql varchar)
                // converted from "\\t"  (raw java string of raw mysql varchar)
                escaped.append("\\t");
                break;
            default:
                // 处理 ASCII 控制字符（ASCII 值小于 32）
                if (c < 32) {
                    // 转义为十六进制格式 \xHH
                    escaped.append(String.format("\\x%02X", (int) c));
                } else {
                    // 普通字符直接追加
                    escaped.append(c);
                }
                break;
            }
        }

        return escaped.toString();
    }
}
