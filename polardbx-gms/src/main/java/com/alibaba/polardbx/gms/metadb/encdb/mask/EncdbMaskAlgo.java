package com.alibaba.polardbx.gms.metadb.encdb.mask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author pangzhaoxing
 */
public class EncdbMaskAlgo {
    private EncdbMaskType maskType;
    private Object[] params;

    public EncdbMaskAlgo(EncdbMaskType maskType, Object[] params) {
        this.maskType = maskType;
        this.params = params;
    }

    public Object[] getParams() {
        return params;
    }

    public EncdbMaskType getMaskType() {
        return maskType;
    }

    public static EncdbMaskAlgo buildEncdbMaskAlgo(EncdbMaskType maskType, Object[] params) {
        switch (maskType) {
        case MASK_DATA_TYPE:
            return new EncdbMaskAlgo(maskType, new Object[0]);
        case MASK_ALL:
            return new EncdbMaskAlgo(maskType, new Object[0]);
        case MASK_FIX_POS:
            if (params.length % 2 != 0) {
                throw new IllegalArgumentException();
            }
            return new EncdbMaskAlgo(maskType,
                Arrays.stream(params).map(p -> Integer.parseInt(String.valueOf(p))).toArray());
        case MASK_FIX_CHAR:
            return new EncdbMaskAlgo(maskType, new Object[] {String.valueOf(params[0])});
        case MASK_EMAIL_PERSON:
            return new EncdbMaskAlgo(maskType, new Object[0]);
        case MASK_EMAIL_PERSON_AND_COMPANY:
            return new EncdbMaskAlgo(maskType, new Object[0]);
        case REPLACE_ALL:
            return new EncdbMaskAlgo(maskType, new Object[] {String.valueOf(params[0])});
        case REPLACE_MAP:
            return new EncdbMaskAlgo(maskType, new Object[] {String.valueOf(params[0]), String.valueOf(params[1])});
        case REPLACE_NUMBER:
            if (params.length == 1) {
                return new EncdbMaskAlgo(maskType, new Object[] {Integer.parseInt(String.valueOf(params[0]))});
            } else if (params.length == 2) {
                return new EncdbMaskAlgo(maskType, new Object[] {
                    Integer.parseInt(String.valueOf(params[0])), Integer.parseInt(String.valueOf(params[1]))});
            }
        case REPLACE_RANDOM:
            if (params == null || params.length == 0) {
                return new EncdbMaskAlgo(maskType, new Object[0]);
            } else if (params.length == 1) {
                return new EncdbMaskAlgo(maskType, new Object[] {String.valueOf(params[0])});
            } else if (params.length == 2) {
                return new EncdbMaskAlgo(maskType, new Object[] {
                    Integer.parseInt(String.valueOf(params[0])), Integer.parseInt(String.valueOf(params[1]))});
            } else {
                return new EncdbMaskAlgo(maskType, new Object[] {
                    Integer.parseInt(String.valueOf(params[0])), Integer.parseInt(String.valueOf(params[1])),
                    String.valueOf(params[2])});
            }
        case TRANSFORM_NUMBER_ROUNDING:
            return new EncdbMaskAlgo(maskType, new Object[0]);
        case TRANSFORM_DATE_ROUNDING:
            return new EncdbMaskAlgo(maskType,
                new Object[] {EncdbMaskType.DateRoundingLevel.valueOf(String.valueOf(params[0]).toUpperCase())});
        case TRANSFORM_STRING_LEFT_SHIFT:
            return new EncdbMaskAlgo(maskType, new Object[] {Integer.parseInt(String.valueOf(params[0]))});
        case TRANSFORM_SHUFFLE:
            return new EncdbMaskAlgo(maskType, new Object[0]);
        default:
            throw new UnsupportedOperationException(maskType.name());
        }
    }

}
