package com.alibaba.polardbx.optimizer.config.table.collation;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;

import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.MIN_MB_EVEN_BYTE_2;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.MIN_MB_ODD_BYTE;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.getGB180304ChsToDiff;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMb1;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMbEven2;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMbEven4;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMbOdd;

public class Gb180302022Unicode520CiCollationHandler extends Gb18030Unicode520CiCollationHandler {
    public Gb180302022Unicode520CiCollationHandler(
        CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    /**
     * GB18030-2022 different GB18030-2005
     * U+9FB4   GB+FE59       U+E81E	GB+82359037
     * U+9FB5   GB+FE61       U+E826	GB+82359038
     * U+9FB6   GB+FE66       U+E82B	GB+82359039
     * U+9FB7   GB+FE67       U+E82C	GB+82359130
     * U+9FB8   GB+FE6D       U+E832	GB+82359131
     * U+9FB9   GB+FE7E       U+E843	GB+82359132
     * U+9FBA   GB+FE90       U+E854	GB+82359133
     * U+9FBB   GB+FEA0       U+E864	GB+82359134
     * <p>
     * U+FE10   GB+A6D9       U+E78D	GB+84318236
     * U+FE12   GB+A6DA       U+E78F	GB+84318237
     * U+FE11   GB+A6DB       U+E78E	GB+84318238
     * U+FE13   GB+A6DC       U+E790	GB+84318239
     * U+FE14   GB+A6DD       U+E791	GB+84318330
     * U+FE15   GB+A6DE       U+E792	GB+84318331
     * U+FE16   GB+A6DF       U+E793	GB+84318332
     * U+FE17   GB+A6EC       U+E794	GB+84318333
     * U+FE18   GB+A6ED       U+E795	GB+84318334
     * U+FE19   GB+A6F3       U+E796	GB+84318335
     */
    private static final int[] tab_gb18030_2022_4_uni_part1 = {
        /* [GB+82358F33 , GB+82359036] */
        0x9FA6, 0x9FA7, 0x9FA8, 0x9FA9, 0x9FAA, 0x9FAB, 0x9FAC, 0x9FAD, 0x9FAE,
        0x9FAF, 0x9FB0, 0x9FB1, 0x9FB2, 0x9FB3,
        /* [GB+82359037, GB+82359134 ] */
        0xE81E, 0xE826, 0xE82B, 0xE82C, 0xE832, 0xE843, 0xE854, 0xE864
    };

    private static final int[] tab_gb18030_2022_4_uni_part2 = {
        /* [GB+84318236, GB+84318335] */
        0xE78D, 0xE78F, 0xE78E, 0xE790, 0xE791, 0xE792, 0xE793, 0xE794, 0xE795,
        0xE796,
        /* [GB+84318336, GB+84318537] */
        0xFE1A, 0xFE1B, 0xFE1C, 0xFE1D, 0xFE1E, 0xFE1F, 0xFE20, 0xFE21, 0xFE22,
        0xFE23, 0xFE24, 0xFE25, 0xFE26, 0xFE27, 0xFE28, 0xFE29, 0xFE2A, 0xFE2B,
        0xFE2C, 0xFE2D, 0xFE2E, 0xFE2F
    };

    private static final int[] tab_uni_gb18030_2022_p1_part1 = {
        /* [U+9FA6 U+9FB3]   unicode-0x5543  [GB+82358F33,GB+82359036]*/
        0x4A63, 0x4A64, 0x4A65, 0x4A66, 0x4A67, 0x4A68, 0x4A69, 0x4A6A, 0x4A6B,
        0x4A6C, 0x4A6D, 0x4A6E, 0x4A6F, 0x4A70,
        /* [U+9FB4, 9FBB] */
        0xFE59, 0xFE61, 0xFE66, 0xFE67, 0xFE6D, 0xFE7E, 0xFE90, 0xFEA0
    };

    private static final int[] tab_uni_gb18030_2022_p2_new = {
        /* [U+FE10, U+FE19]*/
        0xA6D9, 0xA6DB, 0xA6DA, 0xA6DC, 0xA6DD,
        0xA6DE, 0xA6DF, 0xA6EC, 0xA6ED, 0xA6F3
    };

    private final static int GB_2022_PART_1 = tab_gb18030_2022_4_uni_part1.length;
    private final static int GB_2022_PART_2 = tab_gb18030_2022_4_uni_part2.length;

    private final static int[] tab_gb18030_2022_2_uni;
    private final static int[] tab_gb18030_2022_4_uni;

    static {
        tab_gb18030_2022_2_uni = TAB_GB18030_2_UNI.clone();

        tab_gb18030_2022_4_uni = new int[TAB_GB18030_4_UNI.length + GB_2022_PART_1 + GB_2022_PART_2];

        new2022uni(0xFE, 0x59, 0x9FB4);
        new2022uni(0xFE, 0x61, 0x9FB5);
        new2022uni(0xFE, 0x66, 0x9FB6);
        new2022uni(0xFE, 0x67, 0x9FB7);
        new2022uni(0xFE, 0x6D, 0x9FB8);
        new2022uni(0xFE, 0x7E, 0x9FB9);
        new2022uni(0xFE, 0x90, 0x9FBA);
        new2022uni(0xFE, 0xA0, 0x9FBB);

        new2022uni(0xA6, 0xD9, 0xFE10);
        new2022uni(0xA6, 0xDA, 0xFE12);
        new2022uni(0xA6, 0xDB, 0xFE11);
        new2022uni(0xA6, 0xDC, 0xFE13);
        new2022uni(0xA6, 0xDD, 0xFE14);
        new2022uni(0xA6, 0xDE, 0xFE15);
        new2022uni(0xA6, 0xDF, 0xFE16);
        new2022uni(0xA6, 0xEC, 0xFE17);
        new2022uni(0xA6, 0xED, 0xFE18);
        new2022uni(0xA6, 0xF3, 0xFE19);
        /**
         * Add [GB+82359037, GB+82359134] and [GB+84318236, GB+84318335] to
         tab_gb18030_2022_4_uni,
         * split tab_gb18030_4_uni as [0, idx1), [idx1, idx2), [idx2, END]
         * new1 is the index of GB+8235 8F33
         * new2 is the index of GB+8430 9C38
         *

         tab_gb18030_4_uni:
         [GB+81308130, GB+8130D330)
         (GB+8135F436, GB+8137A839)
         (GB+8138FD38, GB+82358F33)
         idx1  -->   [GB+82358F33  [GB+82359037, GB+82359134]
         (GB+8336C738, GB+8336D030)
         (GB+84308534, GB+84309C38)
         idx2  -->  [GB+84318236, GB+84318335] , GB+84318537]
         (GB+84318537, GB+8431A439]
         */

        /* GB18030-2022  4 unicode */
        final int idx1 = 0x4A63 - 6637 - 2110;
        final int idx2 = 0x94BE - 6637 - 2110 - 14426 - 4295;
        System.arraycopy(TAB_GB18030_4_UNI, 0,
            tab_gb18030_2022_4_uni, 0, idx1);
        System.arraycopy(tab_gb18030_2022_4_uni_part1, 0,
            tab_gb18030_2022_4_uni, idx1, GB_2022_PART_1);

        System.arraycopy(TAB_GB18030_4_UNI, idx1,
            tab_gb18030_2022_4_uni, idx1 + GB_2022_PART_1, idx2 - idx1);

        System.arraycopy(tab_gb18030_2022_4_uni_part2, 0,
            tab_gb18030_2022_4_uni, idx2 + GB_2022_PART_1, GB_2022_PART_2);

        System.arraycopy(TAB_GB18030_4_UNI, idx2,
            tab_gb18030_2022_4_uni, idx2 + GB_2022_PART_1 + GB_2022_PART_2, TAB_GB18030_4_UNI.length - idx2);

    }

    private static void new2022uni(int s1, int s2, int uni) {
        int idx = (s1 - MIN_MB_ODD_BYTE) * 192 + (s2 - MIN_MB_EVEN_BYTE_2);
        tab_gb18030_2022_2_uni[idx] = uni;
    }

    @Override
    int getCodePoint(SliceInput sliceInput) {
        return codeOfGB180302022(sliceInput);
    }

    @Override
    public int instr(Slice source, Slice target) {
        return instrForMultiBytes(source, target, Gb180302022Unicode520CiCollationHandler::codeOfGB180302022);
    }

    public static int codeOfGB180302022(SliceInput sliceInput) {
        if (sliceInput.available() <= 0) {
            return INVALID_CODE;
        }

        int idx = 0;
        int cp = 0;
        byte b1 = sliceInput.readByte();

        if (isMb1(b1)) {
            /* [0x00, 0x7F] */
            return Byte.toUnsignedInt(b1);
        } else if (!isMbOdd(b1)) {
            return INVALID_CODE;
        }

        if (!sliceInput.isReadable()) {
            return INVALID_CODE;
        }

        byte b2 = sliceInput.readByte();
        if (isMbEven2(b2)) {
            idx = (Byte.toUnsignedInt(b1) - MIN_MB_ODD_BYTE) * 192 + (Byte.toUnsignedInt(b2) - MIN_MB_EVEN_BYTE_2);
            return tab_gb18030_2022_2_uni[idx];
        } else if (isMbEven4(b2)) {
            if (sliceInput.available() < 2) {
                return INVALID_CODE;
            }

            byte b3 = sliceInput.readByte();
            byte b4 = sliceInput.readByte();
            if (!(isMbOdd(b3) && isMbEven4(b4))) {
                return INVALID_CODE;
            }

            idx = getGB180304ChsToDiff(Slices.wrappedBuffer(b1, b2, b3, b4));

            if (idx < 0x334) /* [GB+81308130, GB+8130D330) */ {
                cp = tab_gb18030_2022_4_uni[idx];
            } else if (idx <= 0x1D20)
                /* [GB+8130D330, GB+8135F436] */ {
                cp = idx + 0x11E;
            } else if (idx < 0x2403)
                /* (GB+8135F436, GB+8137A839) */ {
                cp = tab_gb18030_2022_4_uni[idx - 6637];
            } else if (idx <= 0x2C40)
                /* [GB+8137A839, GB+8138FD38] */ {
                cp = idx + 0x240;
            } else if (idx < 0x4A79)
                /* (GB+8138FD38, GB+82359134] */ {
                cp = tab_gb18030_2022_4_uni[idx - 6637 - 2110];
            } else if (idx <= 0x82BC)
                /* [GB+82358F33, GB+8336C738] */ {
                cp = idx + 0x5543;
            } else if (idx < 0x830E)
                /* (GB+8336C738, GB+8336D030) */ {
                cp = tab_gb18030_2022_4_uni[idx - 6637 - 2110 - 14404];
            } else if (idx <= 0x93D4)
                /* [GB+8336D030, GB+84308534] */ {
                cp = idx + 0x6557;
            } else if (idx < 0x94BE)
                /* (GB+84308534, GB+84309C38) */ {
                cp = tab_gb18030_2022_4_uni[idx - 6637 - 2110 - 14404 - 4295];
            } else if (idx <= 0x98A3)
                /* [GB+84309C38, GB+84318236) */ {
                cp = idx + 0x656C;
            } else if (idx <= 0x99fb)
                /* [GB+84318236, GB+8431A439] */ {
                cp = tab_gb18030_2022_4_uni[idx - 6637 - 2110 - 14404 - 4295 - 998];
            } else if (idx >= 0x2E248 && idx <= 0x12E247)
                /* [GB+90308130, GB+E3329A35] */ {
                cp = idx - 0x1E248;
            } else if ((idx > 0x99fb && idx < 0x2E248) ||
                (idx > 0x12E247 && idx <= 0x18398F))
                /* (GB+8431A439, GB+90308130) and (GB+E3329A35, GB+FE39FE39) */ {
                cp = 0x003F;
            } else {
                return INVALID_CODE;
            }

            return cp;
        } else {
            return MY_CS_ILSEQ;
        }
    }

    @Override
    public CollationName getName() {
        return CollationName.GB18030_2022_UNICODE_520_CI;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.GB18030_2022;
    }
}
