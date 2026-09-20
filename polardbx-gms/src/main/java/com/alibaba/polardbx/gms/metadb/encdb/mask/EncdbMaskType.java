package com.alibaba.polardbx.gms.metadb.encdb.mask;

import com.alibaba.polardbx.gms.metadb.encdb.EncdbRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public enum EncdbMaskType {

    MASK_DATA_TYPE,
    MASK_ALL,
    MASK_FIX_POS,
    MASK_FIX_CHAR,
    MASK_EMAIL_PERSON,
    MASK_EMAIL_PERSON_AND_COMPANY,
    REPLACE_ALL,
    REPLACE_MAP,
    REPLACE_RANDOM,
    REPLACE_NUMBER,
    TRANSFORM_NUMBER_ROUNDING,
    TRANSFORM_DATE_ROUNDING,
    TRANSFORM_STRING_LEFT_SHIFT,
    TRANSFORM_SHUFFLE;

    public static enum DateRoundingLevel {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND
    }

}
