package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.utils.ITransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transaction-scoped binding from a business owner DN to one staging seq.
 */
public final class ExternalStagingTransactionLease {

    private static final ConcurrentHashMap<ITransaction, LeaseState> STATES = new ConcurrentHashMap<>();

    private ExternalStagingTransactionLease() {
    }

    public static int acquire(ITransaction transaction, String dnId) {
        if (transaction == null || !transaction.isDistributedWriteTrx()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Transactional staging requires an XA/TSO write transaction");
        }
        if (dnId == null || dnId.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Transactional staging requires a target DN");
        }

        final LeaseState state;
        synchronized (transaction) {
            LeaseState existing = STATES.get(transaction);
            if (existing == null) {
                existing = new LeaseState();
                final LeaseState registered = existing;
                STATES.put(transaction, existing);
                try {
                    transaction.registerCloseHook(() -> releaseAll(transaction, registered));
                } catch (RuntimeException | Error t) {
                    STATES.remove(transaction, registered);
                    synchronized (registered) {
                        registered.closed = true;
                    }
                    throw t;
                }
            }
            state = existing;
        }

        synchronized (state) {
            if (state.closed) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Transactional staging lease cannot be acquired after transaction close");
            }
            Integer seqId = state.seqByDn.get(dnId);
            if (seqId != null) {
                return seqId;
            }
            seqId = StagingTableManager.getInstance().acquireTransactionLease(dnId);
            state.seqByDn.put(dnId, seqId);
            return seqId;
        }
    }

    public static Map<String, Integer> snapshot(ITransaction transaction) {
        LeaseState state = STATES.get(transaction);
        if (state == null) {
            return new HashMap<>();
        }
        synchronized (state) {
            return new HashMap<>(state.seqByDn);
        }
    }

    private static void releaseAll(ITransaction transaction, LeaseState expected) {
        if (!STATES.remove(transaction, expected)) {
            return;
        }
        final Map<String, Integer> leases;
        synchronized (expected) {
            expected.closed = true;
            leases = new HashMap<>(expected.seqByDn);
            expected.seqByDn.clear();
        }
        for (Integer seqId : leases.values()) {
            StagingTableManager.getInstance().releaseTransactionLease(seqId);
        }
    }

    private static final class LeaseState {
        private final Map<String, Integer> seqByDn = new HashMap<>();
        private boolean closed;
    }
}
