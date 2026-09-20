package com.alibaba.polardbx.gms.ha;

public interface ColumnarHaSwitcher {
    void doHaSwitch(ColumnarHaSwitchParams haSwitchParams);
}
