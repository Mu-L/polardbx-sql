package com.alibaba.polardbx.optimizer.utils;

import org.junit.Test;

/**
 * AONE #84985810: Java UDF compilation fails under JDK 21 because ECJ needs to
 * resolve java.lang.constant.ConstantDesc / Constable (indirectly referenced by
 * java.lang.Long/Integer etc.) but CompileUtils.allowedPackage does not whitelist
 * java/lang/constant/, causing UdfCompilationUnit#findType to return null.
 */
public class CompileUtilsJdk21Test {

    private static final String SUBSTR_FOR_INT_CODE =
        "public class Substrforint extends UserDefinedJavaFunction {\n"
            + "    public Object compute(Object[] args) {\n"
            + "        Long val = (Long) args[0];\n"
            + "        if (val == null) {\n"
            + "            return 0;\n"
            + "        }\n"
            + "        String str = String.valueOf(val);\n"
            + "        Long len = (Long) args[1];\n"
            + "        try {\n"
            + "            Integer n = len.intValue();\n"
            + "            if (n >= str.length()) {\n"
            + "                return Long.valueOf(str);\n"
            + "            }\n"
            + "            int startIndex = str.length() - n;\n"
            + "            String ss = str.substring(startIndex);\n"
            + "            return Long.valueOf(ss);\n"
            + "        } catch (Throwable ex) {\n"
            + "            return 0;\n"
            + "        }\n"
            + "    }\n"
            + "};\n";

    /**
     * AONE #84985810: on JDK 21, ECJ needs to resolve java.lang.constant.ConstantDesc
     * when it processes java.lang.Long usages in the UDF body. CompileUtils.checkInvalidJavaCode
     * must not throw for such a valid UDF that only uses Long/Integer/String.
     */
    @Test
    public void testSubstrforintCompilesUnderCurrentJdk() {
        CompileUtils.checkInvalidJavaCode(SUBSTR_FOR_INT_CODE, "Substrforint");
    }
}
