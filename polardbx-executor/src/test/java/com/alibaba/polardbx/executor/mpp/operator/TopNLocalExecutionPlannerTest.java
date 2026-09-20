package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.planner.PlanUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.runners.Parameterized;

import java.util.List;

public class TopNLocalExecutionPlannerTest extends PlanFragmentTestBase {
    public TopNLocalExecutionPlannerTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                         String lineNum) {
        super(caseName, FragmentRFScheduleTest.class.getSimpleName(), sqlIndex, sql, expectedPlan, lineNum,
            new TopNFragmentTest());
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(TopNLocalExecutionPlannerTest.class);
    }

    private static class TopNFragmentTest extends AbstractPlanFragmentTester {
        public TopNFragmentTest() {
            super(16, 1, 16);
        }

        @Override
        boolean test(List<PipelineFactory> pipelineFactories, ExecutionContext executionContext) {
            StringBuilder builder = new StringBuilder();
            for (PipelineFactory pipelineFactory : pipelineFactories) {
                builder.append(PlanUtils.formatPipelineFragment(executionContext, pipelineFactory,
                    executionContext.getParams().getCurrentParameter()));
            }
            System.out.println(builder);

            Assert.assertTrue(
                areStringsEqualIgnoringFormatting(builder.toString(),
                    "    pipeline=0 dependency=[] parallelism=16\n"
                        + "      OSSTableScan(tables=\"lineitem[p1,p2,p3,p4]\", shardCount=4, sql=\"SELECT `l_orderkey`, `l_shipdate` FROM `lineitem` AS `lineitem` WHERE (`l_shipdate` > ?)\")\n"
                        + "\n"
                        + "    pipeline=1 dependency=[] parallelism=16\n"
                        + "      TopN(sort=\"l_orderkey ASC,l_shipdate ASC\", fetch=+(?2, ?1))\n"
                        + "        RemoteSource(sourceFragmentIds=[0], type=RecordType(INTEGER l_orderkey, DATE l_shipdate))\n"
                        + "\n"
                        + "    pipeline=2 dependency=[] parallelism=1\n"
                        + "      TopN(sort=\"l_orderkey ASC,l_shipdate ASC\", fetch=+(?2, ?1))\n"
                        + "        RemoteSource(sourceFragmentIds=[1], type=RecordType(INTEGER l_orderkey, DATE l_shipdate))\n"
                        + "\n"
                        + "    pipeline=3 dependency=[2] parallelism=1\n"
                        + "      Exchange(distribution=single, collation=[0 ASC-nulls-first, 1 ASC-nulls-first])\n"
                        + "        RemoteSource(sourceFragmentIds=[2], type=RecordType(INTEGER l_orderkey, DATE l_shipdate))\n"
                        + "\n"
                        + "    pipeline=4 dependency=[3] parallelism=1\n"
                        + "      TopN(sort=\"l_orderkey ASC,l_shipdate ASC\", offset=?1, fetch=?2)\n"
                        + "        RemoteSource(sourceFragmentIds=[3], type=RecordType(INTEGER l_orderkey, DATE l_shipdate))\n")
            );

            return true;
        }
    }

    public static boolean areStringsEqualIgnoringFormatting(String str1, String str2) {
        String cleanedStr1 = cleanString(str1);
        String cleanedStr2 = cleanString(str2);

        if (cleanedStr1.length() != cleanedStr2.length()) {
            findDifference(str1, str2);
            return false;
        }

        for (int i = 0; i < cleanedStr1.length(); i++) {
            if (cleanedStr1.charAt(i) != cleanedStr2.charAt(i)) {
                throw GeneralUtil.nestedException(
                    String.format("where's different %d: '%c' (str1) vs '%c' (str2)%n", i, cleanedStr1.charAt(i),
                        cleanedStr2.charAt(i)));
            }
        }

        return true;
    }

    public static String cleanString(String str) {
        return str.trim().replaceAll("[\\s\\p{Punct}]", "");
    }

    public static void findDifference(String str1, String str2) {
        int minLength = Math.min(str1.length(), str2.length());
        for (int i = 0; i < minLength; i++) {
            if (str1.charAt(i) != str2.charAt(i)) {
                throw GeneralUtil.nestedException(
                    String.format("where's different %d: '%c' (str1) vs '%c' (str2)%n", i, str1.charAt(i),
                        str2.charAt(i)));
            }
        }
        if (str1.length() != str2.length()) {
            throw GeneralUtil.nestedException(
                String.format("where's different: %d, str2: %d%n", str1.length(), str2.length()));
        }
    }
}
