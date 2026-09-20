package com.alibaba.polardbx.executor.mpp.server.remotetask;

import io.airlift.http.client.FullJsonResponseHandler;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SimpleHttpResponseHandlerTest {

    private SimpleHttpResponseCallback<Object> callback;
    private URI uri;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        callback = mock(SimpleHttpResponseCallback.class);
        uri = URI.create("http://10.58.119.174:3027/v1/task/test.2.3/status");
    }

    @SuppressWarnings("unchecked")
    private FullJsonResponseHandler.JsonResponse<Object> mockResponse(int statusCode, boolean hasValue) {
        FullJsonResponseHandler.JsonResponse<Object> response = mock(FullJsonResponseHandler.JsonResponse.class);
        when(response.getStatusCode()).thenReturn(statusCode);
        when(response.hasValue()).thenReturn(hasValue);
        when(response.getResponseBody()).thenReturn("");
        when(response.getException()).thenReturn(null);
        if (hasValue) {
            when(response.getValue()).thenReturn(new Object());
        }
        return response;
    }

    @Test
    public void testHttp200WithValue_callsSuccess() {
        SimpleHttpResponseHandler<Object> handler = new SimpleHttpResponseHandler<>(callback, uri);
        Object value = new Object();
        FullJsonResponseHandler.JsonResponse<Object> response = mockResponse(200, true);
        when(response.getValue()).thenReturn(value);

        handler.onSuccess(response);

        verify(callback).success(value);
        verify(callback, never()).failed(any());
        verify(callback, never()).fatal(any());
    }

    @Test
    public void testHttp503_callsFailed() {
        SimpleHttpResponseHandler<Object> handler = new SimpleHttpResponseHandler<>(callback, uri);
        FullJsonResponseHandler.JsonResponse<Object> response = mockResponse(503, false);

        handler.onSuccess(response);

        verify(callback).failed(any());
        verify(callback, never()).success(any());
        verify(callback, never()).fatal(any());
    }

    @Test
    public void testHttp500_callsFailedNotFatal() {
        SimpleHttpResponseHandler<Object> handler = new SimpleHttpResponseHandler<>(callback, uri);
        FullJsonResponseHandler.JsonResponse<Object> response = mockResponse(500, false);

        handler.onSuccess(response);

        // HTTP 500 should be retriable (failed), not fatal
        verify(callback).failed(any());
        verify(callback, never()).fatal(any());
        verify(callback, never()).success(any());
    }

    @Test
    public void testHttp400_callsFatal() {
        SimpleHttpResponseHandler<Object> handler = new SimpleHttpResponseHandler<>(callback, uri);
        FullJsonResponseHandler.JsonResponse<Object> response = mockResponse(400, false);

        handler.onSuccess(response);

        // Non-500/503 errors should still be fatal
        verify(callback).fatal(any());
        verify(callback, never()).failed(any());
        verify(callback, never()).success(any());
    }

    @Test
    public void testHttp200EmptyBody_callsFatal() {
        SimpleHttpResponseHandler<Object> handler = new SimpleHttpResponseHandler<>(callback, uri);
        FullJsonResponseHandler.JsonResponse<Object> response = mockResponse(200, false);

        handler.onSuccess(response);

        verify(callback).fatal(any());
        verify(callback, never()).failed(any());
        verify(callback, never()).success(any());
    }

    @Test
    public void testOnFailure_callsFailed() {
        SimpleHttpResponseHandler<Object> handler = new SimpleHttpResponseHandler<>(callback, uri);
        RuntimeException error = new RuntimeException("connection reset");

        handler.onFailure(error);

        verify(callback).failed(error);
        verify(callback, never()).fatal(any());
        verify(callback, never()).success(any());
    }
}
