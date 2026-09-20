package com.alibaba.polardbx.optimizer.config.table.collation;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;

public class Gb180302022ChineseCiCollationHandler extends Gb18030ChineseCiCollationHandler {
    public Gb180302022ChineseCiCollationHandler(
        CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.GB18030_2022_CHINESE_CI;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.GB18030_2022;
    }

}
