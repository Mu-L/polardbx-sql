package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class FollowerReadRecord extends AbstractLifecycle {
    protected final AtomicLong lastAccessTime;

    protected final ReentrantLock lock;

    private static final FollowerReadRecord INSTANCE = new FollowerReadRecord();

    public static FollowerReadRecord getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        super.doInit();
    }

    public FollowerReadRecord() {
        this.lastAccessTime = new AtomicLong(0);
        this.lock = new ReentrantLock();
    }

    public void access() {
        if (lock.tryLock()) {
            try {
                lastAccessTime.set(System.currentTimeMillis());
            } finally {
                lock.unlock();
            }
        }
    }

    public boolean isExpired() {
        if (DynamicConfig.getInstance().getFollowerRoutingExpireInterval() <= 0) {
            return false;
        }
        long lastTime = lastAccessTime.get();
        long currentTime = System.currentTimeMillis();
        return currentTime >= lastTime + DynamicConfig.getInstance().getFollowerRoutingExpireInterval();
    }
}
