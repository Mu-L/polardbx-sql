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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.function.calc.cobar.CobarPartitionAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.cobar.builder.CobarAlgorithmBuilder;
import com.alibaba.polardbx.optimizer.core.function.calc.cobar.builder.CobarAlgorithmInitParams;
import com.alibaba.polardbx.optimizer.core.function.calc.cobar.builder.CobarAlgorithmType;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.AbstractPartitionAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.AutoPartitionByLong;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.DblePartitionAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.LongRange;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByDate;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByFileMap;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByJumpConsistentHash;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByLong;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByPattern;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.PartitionByString;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.RuleAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.BuildParams;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.DbleAlgorithmBuilder;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.DbleAlgorithmInitParams;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.NumberParseUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.Pair;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.PairUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.PartitionUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.PropertiesUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.ResourceUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.SplitUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.StringUtil;
import org.eclipse.jdt.internal.compiler.DefaultErrorHandlingPolicies;
import org.eclipse.jdt.internal.compiler.IErrorHandlingPolicy;
import org.eclipse.jdt.internal.compiler.IProblemFactory;
import org.eclipse.jdt.internal.compiler.env.ICompilationUnit;
import org.eclipse.jdt.internal.compiler.impl.CompilerOptions;
import org.eclipse.jdt.internal.compiler.problem.DefaultProblemFactory;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CompileUtils {
    public static final String PACKAGE_NAME = "com.alibaba.polardbx.optimizer.core.function.calc.scalar";

    public static final String JAVA_UDF_PATH =
        "com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction";

    public static final String PARAMETERIZED_JAVA_UDF_PATH =
        "com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedParameterizedJavaFunction";

    public static final List<String> allowedPackage = Arrays.asList(
        // basic
        "java/lang/Object.class",
        "java/lang/String.class",
        "java/lang/StringBuffer.class",
        "java/lang/StringBuilder.class",
        "java/lang/CharSequence.class",
        "java/lang/Integer.class",
        "java/lang/Long.class",
        "java/lang/Short.class",
        "java/lang/Double.class",
        "java/lang/Float.class",
        "java/lang/Byte.class",
        "java/lang/Boolean.class",
        "java/lang/Number.class",
        "java/lang/Math.class",
        // JDK 9+: java.lang.Long/Integer/... implement Constable and ECJ needs to resolve
        // java.lang.constant.ConstantDesc when compiling UDFs on JDK 21.
        "java/lang/constant/Constable.class",
        "java/lang/constant/ConstantDesc.class",
        "java/sql/Blob.class",
        "java/sql/Time.class",
        "java/sql/Timestamp.class",
        "java/sql/Date.class",
        "java/nio/Buffer.class",
        "java/nio/ByteBuffer.class",
        "java/nio/ByteOrder.class",
        "java/nio/charset/StandardCharsets.class",
        "java/nio/",
        "java/math/",
        "java/text/",
        "java/time/",
        "java/util/",

        // scalar function
        "com/alibaba/polardbx/optimizer/core/function/calc/",
        "com/alibaba/polardbx/optimizer/core/expression/IExtraFunction.class",
        "com/alibaba/polardbx/optimizer/core/expression/IFunction.class",
        "com/alibaba/polardbx/optimizer/core/expression/IFunction$FunctionType.class",
        "com/alibaba/polardbx/optimizer/core/expression/bean/CoronaFunctionSignature.class",
        "com/alibaba/polardbx/optimizer/core/datatype/DataType.class",
        "com/alibaba/polardbx/optimizer/context/ExecutionContext.class",
        "com/alibaba/polardbx/optimizer/config/table/Field.class",
        "com/alibaba/polardbx/common/charset/CollationName.class",

        // dble
        "com/alibaba/polardbx/optimizer/core/function/calc/dble/",
        "com/alibaba/polardbx/optimizer/core/function/calc/dble/PartitionByLong.class",

        // annotation
        "java/lang/annotation/",

        "java/lang/invoke/MethodHandles.class",
        "java/lang/invoke/MethodHandles$Lookup.class",
        "java/lang/invoke/LambdaMetafactory.class",

        // exception
        "java/lang/NullPointerException.class",
        "java/lang/IndexOutOfBoundsException.class",
        "java/lang/IllegalArgumentException.class",
        "java/lang/ArithmeticException.class",
        "java/lang/InterruptedException.class",
        "java/lang/ClassNotFoundException.class",
        "java/lang/RuntimeException.class",
        "java/lang/Exception.class",
        "java/lang/Throwable.class",
        "java/io/Serializable.class",
        "java/io/IOException.class",
        "java/lang/ArrayIndexOutOfBoundsException.class",
        "java/lang/ClassCastException.class",
        "java/lang/IllegalStateException.class",
        "java/lang/UnsupportedOperationException.class",
        "java/lang/NumberFormatException.class",
        "java/lang/NegativeArraySizeException.class",
        "java/lang/StringIndexOutOfBoundsException.class",
        "java/lang/CloneNotSupportedException.class",
        "java/lang/AssertionError.class",
        "java/lang/Error.class",
//        "java/lang/Class.class",
//        "java/lang/reflect/",

        // basic interface
        "java/lang/AutoCloseable.class",
        "java/lang/Override.class",
        "java/lang/Readable.class",
        "java/lang/Runnable.class",
        "java/lang/FunctionalInterface.class",
        "java/lang/Cloneable.class",
        "java/lang/Comparable.class",
        "java/lang/Character.class",

        "java/io/ByteArrayInputStream.class",
        "java/io/BufferedReader.class",
        "java/io/InputStream.class",
        "java/io/InputStreamReader.class",
        "java/io/ObjectOutputStream.class",
        "java/io/ObjectInputStream.class",
        "java/io/",

        "java/util/regex/Matcher.class",
        "java/util/regex/Pattern.class"

    );

    public static void checkInvalidJavaCode(String userJavaCode, String className) {
        JavaFunctionManager.UdfCompilationUnit compilationUnit =
            new JavaFunctionManager.UdfCompilationUnit(fullJavaCode(userJavaCode), CompileUtils.PACKAGE_NAME,
                className);
//        IErrorHandlingPolicy errorHandlingPolicy = DefaultErrorHandlingPolicies.proceedWithAllProblems();
        IErrorHandlingPolicy errorHandlingPolicy = DefaultErrorHandlingPolicies.exitOnFirstError();
        CompilerOptions compilerOptions = getCompilerOptions();
        IProblemFactory problemFactory = new DefaultProblemFactory();

        org.eclipse.jdt.internal.compiler.Compiler compiler =
            new org.eclipse.jdt.internal.compiler.Compiler(
                compilationUnit,
                errorHandlingPolicy,
                compilerOptions,
                compilationUnit,
                problemFactory);
        compiler.compile(new ICompilationUnit[] {compilationUnit});

        if (compilationUnit.getProblemList().size() != 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                compilationUnit.getProblemList().get(0).getMessage());
        }
    }

    public static CompilerOptions getCompilerOptions() {
        Map settings = new HashMap();
        settings.put(CompilerOptions.OPTION_ReportMissingSerialVersion, CompilerOptions.IGNORE);
        settings.put(CompilerOptions.OPTION_LineNumberAttribute, CompilerOptions.GENERATE);
        settings.put(CompilerOptions.OPTION_SourceFileAttribute, CompilerOptions.GENERATE);
        settings.put(CompilerOptions.OPTION_ReportDeprecation, CompilerOptions.IGNORE);
        settings.put(CompilerOptions.OPTION_ReportUnusedImport, CompilerOptions.IGNORE);
        settings.put(CompilerOptions.OPTION_ReportUnusedParameter, CompilerOptions.IGNORE);
        settings.put(CompilerOptions.OPTION_ReportUncheckedTypeOperation, CompilerOptions.IGNORE);
        settings.put(CompilerOptions.OPTION_ReportUnnecessaryTypeCheck, CompilerOptions.IGNORE);
        settings.put(CompilerOptions.OPTION_ReportNullUncheckedConversion, CompilerOptions.IGNORE);
        settings.put(CompilerOptions.OPTION_ReportNoImplicitStringConversion, CompilerOptions.IGNORE);

        settings.put(CompilerOptions.OPTION_Encoding, "UTF-8");
        settings.put(CompilerOptions.OPTION_LocalVariableAttribute, CompilerOptions.GENERATE);
        String javaVersion = CompilerOptions.VERSION_1_8;
        settings.put(CompilerOptions.OPTION_Source, javaVersion);
        settings.put(CompilerOptions.OPTION_TargetPlatform, javaVersion);
        settings.put(CompilerOptions.OPTION_PreserveUnusedLocal, CompilerOptions.PRESERVE);
        settings.put(CompilerOptions.OPTION_Compliance, javaVersion);
        return new CompilerOptions(settings);
    }

    protected static List<Class> autoImportedJavaClassList = new ArrayList<>();

    static {
//        autoImportedJavaClassList.add(Class.class);
//        autoImportedJavaClassList.add(Serializable.class);
        autoImportedJavaClassList.add(Map.class);
        autoImportedJavaClassList.add(HashMap.class);
        autoImportedJavaClassList.add(TreeMap.class);
        autoImportedJavaClassList.add(List.class);
        autoImportedJavaClassList.add(ArrayList.class);
        autoImportedJavaClassList.add(LinkedList.class);
        autoImportedJavaClassList.add(Set.class);
        autoImportedJavaClassList.add(HashSet.class);
        autoImportedJavaClassList.add(Pattern.class);
        autoImportedJavaClassList.add(ParseException.class);
        autoImportedJavaClassList.add(SimpleDateFormat.class);
        autoImportedJavaClassList.add(Date.class);
        autoImportedJavaClassList.add(Calendar.class);
        autoImportedJavaClassList.add(Collections.class);
        autoImportedJavaClassList.add(BufferedReader.class);
        autoImportedJavaClassList.add(Character.class);
        autoImportedJavaClassList.add(Matcher.class);
        autoImportedJavaClassList.add(Pattern.class);
        autoImportedJavaClassList.add(StandardCharsets.class);
        autoImportedJavaClassList.add(Charset.class);

        autoImportedJavaClassList.add(NumberParseUtil.class);
        autoImportedJavaClassList.add(Pair.class);
        autoImportedJavaClassList.add(PairUtil.class);
        autoImportedJavaClassList.add(PartitionUtil.class);
        autoImportedJavaClassList.add(PropertiesUtil.class);
        autoImportedJavaClassList.add(ResourceUtil.class);
        autoImportedJavaClassList.add(SplitUtil.class);
        autoImportedJavaClassList.add(StringUtil.class);
        autoImportedJavaClassList.add(NumberFormatException.class);
        autoImportedJavaClassList.add(IllegalArgumentException.class);

        autoImportedJavaClassList.add(LongRange.class);
        autoImportedJavaClassList.add(RuleAlgorithm.class);
        autoImportedJavaClassList.add(AbstractPartitionAlgorithm.class);
        autoImportedJavaClassList.add(DblePartitionAlgorithm.class);

        autoImportedJavaClassList.add(PartitionByLong.class);
        autoImportedJavaClassList.add(PartitionByString.class);
        autoImportedJavaClassList.add(PartitionByFileMap.class);
        autoImportedJavaClassList.add(AutoPartitionByLong.class);
        autoImportedJavaClassList.add(PartitionByPattern.class);
        autoImportedJavaClassList.add(PartitionByDate.class);
        autoImportedJavaClassList.add(PartitionByJumpConsistentHash.class);

        autoImportedJavaClassList.add(DbleAlgorithmBuilder.class);
        autoImportedJavaClassList.add(BuildParams.class);
        autoImportedJavaClassList.add(DblePartitionAlgorithm.class);
        autoImportedJavaClassList.add(DbleAlgorithmBuilder.class);
        autoImportedJavaClassList.add(DbleAlgorithmInitParams.class);

        // For Cobar
        autoImportedJavaClassList.add(CobarAlgorithmType.class);
        autoImportedJavaClassList.add(CobarAlgorithmBuilder.class);
        autoImportedJavaClassList.add(CobarAlgorithmInitParams.class);
        autoImportedJavaClassList.add(CobarPartitionAlgorithm.class);

//        autoImportedJavaClassList.add(com.alibaba.fastjson.JSONObject.class);
//        autoImportedJavaClassList.add(com.alibaba.fastjson.JSONArray.class);
//        autoImportedJavaClassList.add(com.alibaba.polardbx.druid.support.json.JSONUtils.class);

    }

    public static String fullJavaCode(String userJavaCode) {
        StringBuilder sb = new StringBuilder();
        sb.append("package ").append(CompileUtils.PACKAGE_NAME).append(";\n");
        sb.append("import ").append(CompileUtils.JAVA_UDF_PATH).append(";\n");
        sb.append("import ").append(CompileUtils.PARAMETERIZED_JAVA_UDF_PATH).append(";\n");
        for (int i = 0; i < autoImportedJavaClassList.size(); i++) {
            sb.append("import ").append(autoImportedJavaClassList.get(i).getName()).append(";\n");
        }
        sb.append(userJavaCode);
        return sb.toString();
    }
}

