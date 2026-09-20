package com.alibaba.polardbx.optimizer.config.table.collation;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;

public class Gb180302022BinCollationHandler extends Gb18030BinCollationHandler {
    public Gb180302022BinCollationHandler(
        CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.GB18030_2022_BIN;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.GB18030_2022;
    }

}
