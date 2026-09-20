package com.alibaba.polardbx.rpc.columnar;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.71.0)",
    comments = "Source: delta_proto.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ColumnarDeltaServiceGrpc {

  private ColumnarDeltaServiceGrpc() {}

  public static final String SERVICE_NAME = "orc.proto.ColumnarDeltaService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<ColumnarDeltaRequest,
      ColumnarDeltaResponse> getColumnarDeltaStreamMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ColumnarDeltaStream",
      requestType = ColumnarDeltaRequest.class,
      responseType = ColumnarDeltaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<ColumnarDeltaRequest,
      ColumnarDeltaResponse> getColumnarDeltaStreamMethod() {
    io.grpc.MethodDescriptor<ColumnarDeltaRequest, ColumnarDeltaResponse> getColumnarDeltaStreamMethod;
    if ((getColumnarDeltaStreamMethod = ColumnarDeltaServiceGrpc.getColumnarDeltaStreamMethod) == null) {
      synchronized (ColumnarDeltaServiceGrpc.class) {
        if ((getColumnarDeltaStreamMethod = ColumnarDeltaServiceGrpc.getColumnarDeltaStreamMethod) == null) {
          ColumnarDeltaServiceGrpc.getColumnarDeltaStreamMethod = getColumnarDeltaStreamMethod =
              io.grpc.MethodDescriptor.<ColumnarDeltaRequest, ColumnarDeltaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ColumnarDeltaStream"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ColumnarDeltaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ColumnarDeltaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ColumnarDeltaServiceMethodDescriptorSupplier("ColumnarDeltaStream"))
              .build();
        }
      }
    }
    return getColumnarDeltaStreamMethod;
  }

  private static volatile io.grpc.MethodDescriptor<ColumnarCacheFilesInfoRequest,
      ColumnarCacheFilesInfoResponse> getColumnarCacheFilesInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ColumnarCacheFilesInfo",
      requestType = ColumnarCacheFilesInfoRequest.class,
      responseType = ColumnarCacheFilesInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<ColumnarCacheFilesInfoRequest,
      ColumnarCacheFilesInfoResponse> getColumnarCacheFilesInfoMethod() {
    io.grpc.MethodDescriptor<ColumnarCacheFilesInfoRequest, ColumnarCacheFilesInfoResponse> getColumnarCacheFilesInfoMethod;
    if ((getColumnarCacheFilesInfoMethod = ColumnarDeltaServiceGrpc.getColumnarCacheFilesInfoMethod) == null) {
      synchronized (ColumnarDeltaServiceGrpc.class) {
        if ((getColumnarCacheFilesInfoMethod = ColumnarDeltaServiceGrpc.getColumnarCacheFilesInfoMethod) == null) {
          ColumnarDeltaServiceGrpc.getColumnarCacheFilesInfoMethod = getColumnarCacheFilesInfoMethod =
              io.grpc.MethodDescriptor.<ColumnarCacheFilesInfoRequest, ColumnarCacheFilesInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ColumnarCacheFilesInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ColumnarCacheFilesInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ColumnarCacheFilesInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ColumnarDeltaServiceMethodDescriptorSupplier("ColumnarCacheFilesInfo"))
              .build();
        }
      }
    }
    return getColumnarCacheFilesInfoMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ColumnarDeltaServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ColumnarDeltaServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ColumnarDeltaServiceStub>() {
        @Override
        public ColumnarDeltaServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ColumnarDeltaServiceStub(channel, callOptions);
        }
      };
    return ColumnarDeltaServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static ColumnarDeltaServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ColumnarDeltaServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ColumnarDeltaServiceBlockingV2Stub>() {
        @Override
        public ColumnarDeltaServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ColumnarDeltaServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return ColumnarDeltaServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ColumnarDeltaServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ColumnarDeltaServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ColumnarDeltaServiceBlockingStub>() {
        @Override
        public ColumnarDeltaServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ColumnarDeltaServiceBlockingStub(channel, callOptions);
        }
      };
    return ColumnarDeltaServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ColumnarDeltaServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ColumnarDeltaServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ColumnarDeltaServiceFutureStub>() {
        @Override
        public ColumnarDeltaServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ColumnarDeltaServiceFutureStub(channel, callOptions);
        }
      };
    return ColumnarDeltaServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void columnarDeltaStream(ColumnarDeltaRequest request,
                                     io.grpc.stub.StreamObserver<ColumnarDeltaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getColumnarDeltaStreamMethod(), responseObserver);
    }

    /**
     */
    default void columnarCacheFilesInfo(ColumnarCacheFilesInfoRequest request,
                                        io.grpc.stub.StreamObserver<ColumnarCacheFilesInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getColumnarCacheFilesInfoMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ColumnarDeltaService.
   */
  public static abstract class ColumnarDeltaServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return ColumnarDeltaServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ColumnarDeltaService.
   */
  public static final class ColumnarDeltaServiceStub
      extends io.grpc.stub.AbstractAsyncStub<ColumnarDeltaServiceStub> {
    private ColumnarDeltaServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected ColumnarDeltaServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ColumnarDeltaServiceStub(channel, callOptions);
    }

    /**
     */
    public void columnarDeltaStream(ColumnarDeltaRequest request,
                                    io.grpc.stub.StreamObserver<ColumnarDeltaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getColumnarDeltaStreamMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void columnarCacheFilesInfo(ColumnarCacheFilesInfoRequest request,
                                       io.grpc.stub.StreamObserver<ColumnarCacheFilesInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getColumnarCacheFilesInfoMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ColumnarDeltaService.
   */
  public static final class ColumnarDeltaServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<ColumnarDeltaServiceBlockingV2Stub> {
    private ColumnarDeltaServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected ColumnarDeltaServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ColumnarDeltaServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, ColumnarDeltaResponse>
        columnarDeltaStream(ColumnarDeltaRequest request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getColumnarDeltaStreamMethod(), getCallOptions(), request);
    }

    /**
     */
    public ColumnarCacheFilesInfoResponse columnarCacheFilesInfo(ColumnarCacheFilesInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getColumnarCacheFilesInfoMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service ColumnarDeltaService.
   */
  public static final class ColumnarDeltaServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ColumnarDeltaServiceBlockingStub> {
    private ColumnarDeltaServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected ColumnarDeltaServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ColumnarDeltaServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<ColumnarDeltaResponse> columnarDeltaStream(
        ColumnarDeltaRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getColumnarDeltaStreamMethod(), getCallOptions(), request);
    }

    /**
     */
    public ColumnarCacheFilesInfoResponse columnarCacheFilesInfo(ColumnarCacheFilesInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getColumnarCacheFilesInfoMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ColumnarDeltaService.
   */
  public static final class ColumnarDeltaServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<ColumnarDeltaServiceFutureStub> {
    private ColumnarDeltaServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected ColumnarDeltaServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ColumnarDeltaServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<ColumnarCacheFilesInfoResponse> columnarCacheFilesInfo(
        ColumnarCacheFilesInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getColumnarCacheFilesInfoMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_COLUMNAR_DELTA_STREAM = 0;
  private static final int METHODID_COLUMNAR_CACHE_FILES_INFO = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_COLUMNAR_DELTA_STREAM:
          serviceImpl.columnarDeltaStream((ColumnarDeltaRequest) request,
              (io.grpc.stub.StreamObserver<ColumnarDeltaResponse>) responseObserver);
          break;
        case METHODID_COLUMNAR_CACHE_FILES_INFO:
          serviceImpl.columnarCacheFilesInfo((ColumnarCacheFilesInfoRequest) request,
              (io.grpc.stub.StreamObserver<ColumnarCacheFilesInfoResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getColumnarDeltaStreamMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              ColumnarDeltaRequest,
              ColumnarDeltaResponse>(
                service, METHODID_COLUMNAR_DELTA_STREAM)))
        .addMethod(
          getColumnarCacheFilesInfoMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              ColumnarCacheFilesInfoRequest,
              ColumnarCacheFilesInfoResponse>(
                service, METHODID_COLUMNAR_CACHE_FILES_INFO)))
        .build();
  }

  private static abstract class ColumnarDeltaServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ColumnarDeltaServiceBaseDescriptorSupplier() {}

    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return DeltaProto.getDescriptor();
    }

    @Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ColumnarDeltaService");
    }
  }

  private static final class ColumnarDeltaServiceFileDescriptorSupplier
      extends ColumnarDeltaServiceBaseDescriptorSupplier {
    ColumnarDeltaServiceFileDescriptorSupplier() {}
  }

  private static final class ColumnarDeltaServiceMethodDescriptorSupplier
      extends ColumnarDeltaServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ColumnarDeltaServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ColumnarDeltaServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ColumnarDeltaServiceFileDescriptorSupplier())
              .addMethod(getColumnarDeltaStreamMethod())
              .addMethod(getColumnarCacheFilesInfoMethod())
              .build();
        }
      }
    }
    return result;
  }
}
