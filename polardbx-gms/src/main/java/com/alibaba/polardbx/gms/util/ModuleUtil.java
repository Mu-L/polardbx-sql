package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.jdbc.ParameterContext;

import java.util.Map;

/**
 * @author fangwu
 */
public class ModuleUtil {

    /**
     * Builds a log string of parameters for logging the current parameter contexts.
     *
     * @param parameterMap The map containing current parameter contexts.
     * @return A log string of parameters.
     */
    public static String buildParametersLog(Map<Integer, ParameterContext> parameterMap) {
        // If the parameter map is null or empty, return an empty string
        if (parameterMap == null || parameterMap.isEmpty()) {
            return "";
        }

        // Use StringBuilder to construct the log string
        StringBuilder logBuilder = new StringBuilder();

        // Iterate through each element in the parameter map
        for (Integer key : parameterMap.keySet()) {
            ParameterContext context = parameterMap.get(key);

            // If the context is not null, append its string representation to the log builder
            if (context != null) {
                logBuilder.append(context.toString());
                logBuilder.append("-");
            }
        }

        // Remove the last unnecessary separator
        if (logBuilder.length() >= 1) {
            logBuilder.setLength(logBuilder.length() - 1);
        }

        // Return the truncated log string, ensuring it does not exceed the maximum length limit
        int maxLength = 1024 * 8;
        if (logBuilder.length() > maxLength) {
            return logBuilder.substring(0, maxLength);
        } else {
            return logBuilder.toString();
        }
    }
}
