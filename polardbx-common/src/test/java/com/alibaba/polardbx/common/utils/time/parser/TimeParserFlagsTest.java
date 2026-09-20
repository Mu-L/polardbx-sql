package com.alibaba.polardbx.common.utils.time.parser;

import com.alibaba.polardbx.common.utils.Assert;
import org.junit.Test;

/**
 * Unit tests for TimeParserFlags class.
 */
public class TimeParserFlagsTest {

    /**
     * Test FLAG_TIME_TRUNCATE_FRACTIONAL constant value and basic operations.
     */
    @Test
    public void testTimeTruncateFractionalFlag() {
        // Verify the constant value
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL == 64);
        
        // Test basic check operation
        Assert.assertTrue(TimeParserFlags.check(64, TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL));
        Assert.assertTrue(!TimeParserFlags.check(0, TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL));
        Assert.assertTrue(TimeParserFlags.check(65, TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL));
        
        // Test combination with other flags
        int combinedFlags = TimeParserFlags.FLAG_TIME_FUZZY_DATE | 
                           TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL |
                           TimeParserFlags.FLAG_TIME_NO_ZERO_DATE;
                           
        Assert.assertTrue(TimeParserFlags.check(combinedFlags, TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL));
        Assert.assertTrue(TimeParserFlags.check(combinedFlags, TimeParserFlags.FLAG_TIME_FUZZY_DATE));
        Assert.assertTrue(TimeParserFlags.check(combinedFlags, TimeParserFlags.FLAG_TIME_NO_ZERO_DATE));
        
        // Test removing the flag
        int withoutTruncateFlag = combinedFlags & ~TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL;
        Assert.assertTrue(!TimeParserFlags.check(withoutTruncateFlag, TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL));
        Assert.assertTrue(TimeParserFlags.check(withoutTruncateFlag, TimeParserFlags.FLAG_TIME_FUZZY_DATE));
        Assert.assertTrue(TimeParserFlags.check(withoutTruncateFlag, TimeParserFlags.FLAG_TIME_NO_ZERO_DATE));
    }
    
    /**
     * Test all TimeParserFlags constants to ensure they have correct values.
     */
    @Test
    public void testAllTimeParserFlagsValues() {
        // Basic time parsing flags
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_FUZZY_DATE == 1);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_DATETIME_ONLY == 2);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_NO_NANO_ROUNDING == 4);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_NO_DATE_FRAC_WARN == 8);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_NO_ZERO_IN_DATE == 16);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_NO_ZERO_DATE == 32);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_TRUNCATE_FRACTIONAL == 64);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_INVALID_DATES == 64); // Same value as above
        
        // Week format flags
        Assert.assertTrue(TimeParserFlags.FLAG_WEEK_MONDAY_FIRST == 1);
        Assert.assertTrue(TimeParserFlags.FLAG_WEEK_YEAR == 2);
        Assert.assertTrue(TimeParserFlags.FLAG_WEEK_FIRST_WEEKDAY == 4);
        
        // Warning flags
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_WARN_TRUNCATED == 1);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_WARN_OUT_OF_RANGE == 2);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_WARN_INVALID_TIMESTAMP == 4);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_WARN_ZERO_DATE == 8);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_NOTE_TRUNCATED == 16);
        Assert.assertTrue(TimeParserFlags.FLAG_TIME_WARN_ZERO_IN_DATE == 32);
    }
}