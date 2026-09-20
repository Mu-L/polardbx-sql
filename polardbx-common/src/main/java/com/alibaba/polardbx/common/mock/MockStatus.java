package com.alibaba.polardbx.common.mock;

import lombok.Getter;

import java.io.Closeable;

public class MockStatus implements Closeable {
    @Getter
    private static volatile boolean mock = false;

    public MockStatus() {
        setMock(true);
    }

    private static void setMock(boolean mock) {
        MockStatus.mock = mock;
    }

    @Override
    public void close() {
        setMock(false);
    }
}
