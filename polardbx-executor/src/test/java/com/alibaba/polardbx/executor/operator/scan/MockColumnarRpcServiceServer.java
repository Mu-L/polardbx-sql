package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.rpc.columnar.ColumnarDeltaRequest;
import com.alibaba.polardbx.rpc.columnar.ColumnarDeltaResponse;
import com.alibaba.polardbx.rpc.columnar.ColumnarDeltaServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.openMockFile;

public class MockColumnarRpcServiceServer extends ColumnarDeltaServiceGrpc.ColumnarDeltaServiceImplBase {
    private static final int BUFFER_SIZE = 1024;

    final private boolean randomFail;

    public MockColumnarRpcServiceServer() {
        this(false);
    }

    public MockColumnarRpcServiceServer(boolean randomFail) {
        this.randomFail = randomFail;
    }

    @Override
    public void columnarDeltaStream(ColumnarDeltaRequest request,
                                    StreamObserver<ColumnarDeltaResponse> responseObserver) {
        String fileName = request.getFileName();
        int offset = request.getOffset();
        int length = request.getLength();

        Random rand = new Random();

        try (FSDataInputStream fis = openMockFile(fileName)) {
            // Move to the requested offset
            fis.skip(offset);
            int bytesRead = 0;
            while (bytesRead < length) {
                if (randomFail && rand.nextBoolean()) {
                    switch (rand.nextInt(5)) {
                    case 0:
                        responseObserver.onNext(ColumnarDeltaResponse.newBuilder()
                            .setStatus(ColumnarDeltaResponse.Status.INVALID_ARGUMENT).build());
                        break;
                    case 1:
                        responseObserver.onNext(ColumnarDeltaResponse.newBuilder()
                            .setStatus(ColumnarDeltaResponse.Status.NOT_FOUND).build());
                        break;
                    case 2:
                        responseObserver.onNext(ColumnarDeltaResponse.newBuilder()
                            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED).build());
                        break;
                    case 3:
                        responseObserver.onNext(ColumnarDeltaResponse.newBuilder()
                            .setStatus(ColumnarDeltaResponse.Status.RATE_LIMITED).build());
                        break;
                    case 4:
                        responseObserver.onNext(ColumnarDeltaResponse.newBuilder()
                            .setStatus(ColumnarDeltaResponse.Status.INTERNAL_ERROR).build());
                        break;
                    default:
                        break;
                    }
                    return;
                }
                int chunkSize = Math.min(BUFFER_SIZE, length - bytesRead);
                byte[] buffer = new byte[chunkSize];
                int currentBytesRead = fis.read(buffer);
                if (currentBytesRead != -1) {
                    responseObserver.onNext(ColumnarDeltaResponse.newBuilder()
                        .setStatus(ColumnarDeltaResponse.Status.OK)
                        .setData(com.google.protobuf.ByteString.copyFrom(Arrays.copyOf(buffer, currentBytesRead)))
                        .build());
                } else {
                    responseObserver.onNext(ColumnarDeltaResponse.newBuilder()
                        .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
                        .setErrorMessage("Could not read data.")
                        .build());
                }
                bytesRead += currentBytesRead;
            }
        } catch (IOException e) {
            responseObserver.onNext(ColumnarDeltaResponse.newBuilder()
                .setStatus(ColumnarDeltaResponse.Status.INTERNAL_ERROR)
                .setErrorMessage("Internal error: " + e.getMessage())
                .build());
        } finally {
            responseObserver.onCompleted();
        }
    }
}
