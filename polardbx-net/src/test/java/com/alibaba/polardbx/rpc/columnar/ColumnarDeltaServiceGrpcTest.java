package com.alibaba.polardbx.rpc.columnar;

import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class ColumnarDeltaServiceGrpcTest {

    // 测试 getServiceDescriptor 方法
    @Test
    public void testGetServiceDescriptor() {
        ServiceDescriptor descriptor = ColumnarDeltaServiceGrpc.getServiceDescriptor();
        assertNotNull(descriptor);
        assertEquals("orc.proto.ColumnarDeltaService", descriptor.getName());
    }

    // 测试 getColumnarDeltaStreamMethod 方法
    @Test
    public void testGetColumnarDeltaStreamMethod() {
        MethodDescriptor<ColumnarDeltaRequest, ColumnarDeltaResponse> methodDescriptor = 
            ColumnarDeltaServiceGrpc.getColumnarDeltaStreamMethod();
        assertNotNull(methodDescriptor);
        assertEquals(MethodDescriptor.MethodType.SERVER_STREAMING, methodDescriptor.getType());
        assertEquals("orc.proto.ColumnarDeltaService/ColumnarDeltaStream", methodDescriptor.getFullMethodName());
    }

    // 测试 getColumnarCacheFilesInfoMethod 方法
    @Test
    public void testGetColumnarCacheFilesInfoMethod() {
        MethodDescriptor<ColumnarCacheFilesInfoRequest, ColumnarCacheFilesInfoResponse> methodDescriptor = 
            ColumnarDeltaServiceGrpc.getColumnarCacheFilesInfoMethod();
        assertNotNull(methodDescriptor);
        assertEquals(MethodDescriptor.MethodType.UNARY, methodDescriptor.getType());
        assertEquals("orc.proto.ColumnarDeltaService/ColumnarCacheFilesInfo", methodDescriptor.getFullMethodName());
    }

    // 测试 newStub 方法
    @Test
    public void testNewStub() {
        Channel channel = mock(Channel.class);
        ColumnarDeltaServiceGrpc.ColumnarDeltaServiceStub stub = ColumnarDeltaServiceGrpc.newStub(channel);
        assertNotNull(stub);
    }

    // 测试 newBlockingStub 方法
    @Test
    public void testNewBlockingStub() {
        Channel channel = mock(Channel.class);
        ColumnarDeltaServiceGrpc.ColumnarDeltaServiceBlockingStub stub = ColumnarDeltaServiceGrpc.newBlockingStub(channel);
        assertNotNull(stub);
    }

    // 测试 newBlockingV2Stub 方法
    @Test
    public void testNewBlockingV2Stub() {
        Channel channel = mock(Channel.class);
        ColumnarDeltaServiceGrpc.ColumnarDeltaServiceBlockingV2Stub stub = ColumnarDeltaServiceGrpc.newBlockingV2Stub(channel);
        assertNotNull(stub);
    }

    // 测试 newFutureStub 方法
    @Test
    public void testNewFutureStub() {
        Channel channel = mock(Channel.class);
        ColumnarDeltaServiceGrpc.ColumnarDeltaServiceFutureStub stub = ColumnarDeltaServiceGrpc.newFutureStub(channel);
        assertNotNull(stub);
    }

    // 测试 ColumnarDeltaServiceImplBase 构造函数
    @Test
    public void testColumnarDeltaServiceImplBaseConstructor() {
        ColumnarDeltaServiceGrpc.ColumnarDeltaServiceImplBase implBase = 
            new ColumnarDeltaServiceGrpc.ColumnarDeltaServiceImplBase() {};
        assertNotNull(implBase);
    }

    // 测试 ColumnarDeltaServiceImplBase bindService 方法
    @Test
    public void testColumnarDeltaServiceImplBaseBindService() {
        ColumnarDeltaServiceGrpc.ColumnarDeltaServiceImplBase implBase = 
            new ColumnarDeltaServiceGrpc.ColumnarDeltaServiceImplBase() {};
        
        assertNotNull(implBase.bindService());
    }

    // 测试 AsyncService 默认实现的 columnarDeltaStream 方法
    @Test
    public void testAsyncServiceColumnarDeltaStreamDefault() {
        ColumnarDeltaServiceGrpc.AsyncService asyncService = new ColumnarDeltaServiceGrpc.AsyncService() {};
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder().build();
        StreamObserver<ColumnarDeltaResponse> responseObserver = mock(StreamObserver.class);
        
        // 调用默认实现，应该不会抛出异常
        asyncService.columnarDeltaStream(request, responseObserver);
    }

    // 测试 AsyncService 默认实现的 columnarCacheFilesInfo 方法
    @Test
    public void testAsyncServiceColumnarCacheFilesInfoDefault() {
        ColumnarDeltaServiceGrpc.AsyncService asyncService = new ColumnarDeltaServiceGrpc.AsyncService() {};
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.newBuilder().build();
        StreamObserver<ColumnarCacheFilesInfoResponse> responseObserver = mock(StreamObserver.class);
        
        // 调用默认实现，应该不会抛出异常
        asyncService.columnarCacheFilesInfo(request, responseObserver);
    }

    // 测试 bindService 方法
    @Test
    public void testBindService() {
        ColumnarDeltaServiceGrpc.AsyncService asyncService = new ColumnarDeltaServiceGrpc.AsyncService() {};
        assertNotNull(ColumnarDeltaServiceGrpc.bindService(asyncService));
    }

    // 测试服务名称常量
    @Test
    public void testServiceNameConstant() {
        assertEquals("orc.proto.ColumnarDeltaService", ColumnarDeltaServiceGrpc.SERVICE_NAME);
    }

    // 测试 getColumnarDeltaStreamMethod 方法的双重检查锁定
    @Test
    public void testGetColumnarDeltaStreamMethodDoubleCheckLocking() {
        // 第一次调用
        MethodDescriptor<ColumnarDeltaRequest, ColumnarDeltaResponse> method1 = 
            ColumnarDeltaServiceGrpc.getColumnarDeltaStreamMethod();
        
        // 第二次调用，应该返回相同的实例
        MethodDescriptor<ColumnarDeltaRequest, ColumnarDeltaResponse> method2 = 
            ColumnarDeltaServiceGrpc.getColumnarDeltaStreamMethod();
        
        assertSame(method1, method2);
    }

    // 测试 getColumnarCacheFilesInfoMethod 方法的双重检查锁定
    @Test
    public void testGetColumnarCacheFilesInfoMethodDoubleCheckLocking() {
        // 第一次调用
        MethodDescriptor<ColumnarCacheFilesInfoRequest, ColumnarCacheFilesInfoResponse> method1 = 
            ColumnarDeltaServiceGrpc.getColumnarCacheFilesInfoMethod();
        
        // 第二次调用，应该返回相同的实例
        MethodDescriptor<ColumnarCacheFilesInfoRequest, ColumnarCacheFilesInfoResponse> method2 = 
            ColumnarDeltaServiceGrpc.getColumnarCacheFilesInfoMethod();
        
        assertSame(method1, method2);
    }

    // 测试 getServiceDescriptor 方法的双重检查锁定
    @Test
    public void testGetServiceDescriptorDoubleCheckLocking() {
        // 第一次调用
        ServiceDescriptor descriptor1 = ColumnarDeltaServiceGrpc.getServiceDescriptor();
        
        // 第二次调用，应该返回相同的实例
        ServiceDescriptor descriptor2 = ColumnarDeltaServiceGrpc.getServiceDescriptor();
        
        assertSame(descriptor1, descriptor2);
    }
}