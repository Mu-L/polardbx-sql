package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Unit test to verify that the env80 baseline for hash_window_simple
 * has correct avg precision for SINGLE tables.
 * <p>
 * Bug: env80 hash_window_simple.result has avg values with 9 decimal places
 * (e.g., 1.000000000) instead of the expected 4 decimal places (e.g., 1.0000)
 * that match the env baseline behavior.
 * <p>
 * AONE: 82183457
 */
public class PartitionTableWindowBaselineTest {

    private static final String ENV80_RESULT_PATH =
        "partition/env80/PartitionTableWindowTest/hash_window_simple.result";

    /**
     * Pattern to match avg column values in the t_simple table section.
     * The correct format should be X.XXXX (4 decimal places), not X.XXXXXXXXX (9 decimal places).
     */
    private static final Pattern SINGLE_TABLE_AVG_PATTERN =
        Pattern.compile("^(\\d+),(\\d+),(\\d+),(\\d+\\.\\d+)$");

    @Test
    public void testSingleTableAvgPrecision() throws Exception {
        InputStream in = getClass().getClassLoader().getResourceAsStream(ENV80_RESULT_PATH);
        Assert.assertNotNull("env80 hash_window_simple.result not found", in);

        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf8"));
        boolean inSimpleSection = false;
        String line;
        int lineNum = 0;
        StringBuilder wrongLines = new StringBuilder();

        while ((line = reader.readLine()) != null) {
            lineNum++;

            // Detect the t_simple table section
            if (line.contains("ENGINE = InnoDB SINGLE")) {
                inSimpleSection = true;
                continue;
            }

            // Exit section when we hit the next CREATE TABLE
            if (inSimpleSection && line.toUpperCase().startsWith("CREATE TABLE")) {
                break;
            }

            if (inSimpleSection) {
                // Skip column headers and INSERT statements
                if (line.startsWith("c1,c2,sum1,avg1") || line.startsWith("insert into") || line.isEmpty()) {
                    continue;
                }

                // Check avg values in data rows
                Matcher matcher = SINGLE_TABLE_AVG_PATTERN.matcher(line);
                if (matcher.matches()) {
                    String avgValue = matcher.group(4);
                    int decimalPlaces = avgValue.substring(avgValue.indexOf('.') + 1).length();

                    // The expected precision is 4 decimal places (like env baseline: 1.0000)
                    // Not 9 decimal places (current bug: 1.000000000)
                    if (decimalPlaces != 4) {
                        wrongLines.append(String.format(
                            "Line %d: avg value '%s' has %d decimal places, expected 4%n",
                            lineNum, avgValue, decimalPlaces));
                    }
                }
            }
        }

        Assert.assertTrue(
            "env80 hash_window_simple.result SINGLE table avg values have wrong precision " +
                "(expected 4 decimal places like env baseline, but found):\n" + wrongLines.toString(),
            wrongLines.length() == 0);
    }
}
