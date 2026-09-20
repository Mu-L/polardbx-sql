package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.blob.BlobRef;

/**
 * Validates the lifecycle metadata returned by a single externalized Blob write.
 */
public final class BlobWriteReceiptValidator {

    private BlobWriteReceiptValidator() {
    }

    public static BlobWriter.WriteResult validate(long expectedTableId, BlobWriter.WriteResult result,
                                                  String context) {
        if (result == null) {
            throw invalid(context, "result is null");
        }
        if (!isStructurallyValidBlobRef(result.getBlobRefHex())) {
            throw invalid(context, "result has an invalid BlobRef");
        }
        if (result.getFuture() == null) {
            throw invalid(context, "result has no upload future");
        }
        if (BlobRef.decodeSlotAddr(result.getBlobRefHex()) != result.getBlobAddr()) {
            throw invalid(context, "BlobRef address does not match result address " + result.getBlobAddr());
        }
        if (BlobRef.decodeSeqId(result.getBlobRefHex()) != result.getSeqId()) {
            throw invalid(context, "BlobRef sequence does not match result sequence " + result.getSeqId());
        }
        if (result.getTableId() != expectedTableId) {
            throw invalid(context, "result tableId " + result.getTableId()
                + " does not match input tableId " + expectedTableId);
        }
        return result;
    }

    private static boolean isStructurallyValidBlobRef(String blobRefHex) {
        return BlobRef.isVersion2(blobRefHex);
    }

    private static TddlRuntimeException invalid(String context, String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
            "Invalid externalized Blob write receipt for " + context + ": " + detail);
    }
}
