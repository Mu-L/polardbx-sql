package com.alibaba.polardbx.optimizer.config.table.charset;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.config.table.collation.Gb180302022BinCollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Gb180302022ChineseCiCollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Gb180302022Unicode520CiCollationHandler;

public class Gb180302022CharsetHandler extends AbstractCharsetHandler {
    public Gb180302022CharsetHandler(CollationName checkedCollationName) {
        super(CharsetName.GB18030_2022.toJavaCharset(), checkedCollationName);
        switch (checkedCollationName) {
        case GB18030_2022_CHINESE_CI:
            this.collationHandler = new Gb180302022ChineseCiCollationHandler(this);
            break;
        case GB18030_2022_BIN:
            this.collationHandler = new Gb180302022BinCollationHandler(this);
            break;
        case GB18030_2022_UNICODE_520_CI:
            this.collationHandler = new Gb180302022Unicode520CiCollationHandler(this);
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public CharsetName getName() {
        return CharsetName.GB18030_2022;
    }

    @Override
    public int maxLenOfMultiBytes() {
        return 4;
    }

    @Override
    public int minLenOfMultiBytes() {
        return 1;
    }
}
