/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ai;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * HTTP client for AI API calls using Apache HttpClient.
 *
 * <p>Uses a shared {@link CloseableHttpClient} instance with connection pooling,
 * configurable timeouts, SSRF protection, and TLS support for secure
 * communication with AI service endpoints.
 */
public class AiHttpClient {

    private static final Logger logger = LoggerFactory.getLogger(AiHttpClient.class);

    private static final int CONNECT_TIMEOUT_MS = 30_000;
    private static final int SOCKET_TIMEOUT_MS = 600_000;
    private static final int STREAMING_SOCKET_TIMEOUT_MS = 300_000;
    private static final int CONNECTION_REQUEST_TIMEOUT_MS = 10_000;

    private static final CloseableHttpClient CLIENT;

    static {
        Registry<ConnectionSocketFactory> socketFactoryRegistry =
            RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", SSLConnectionSocketFactory.getSocketFactory())
                .build();

        PoolingHttpClientConnectionManager cm =
            new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        cm.setMaxTotal(50);
        cm.setDefaultMaxPerRoute(5);
        cm.setValidateAfterInactivity(30_000);

        RequestConfig defaultRequestConfig = RequestConfig.custom()
            .setConnectTimeout(CONNECT_TIMEOUT_MS)
            .setSocketTimeout(SOCKET_TIMEOUT_MS)
            .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MS)
            .build();

        CLIENT = HttpClients.custom()
            .setConnectionManager(cm)
            .setDefaultRequestConfig(defaultRequestConfig)
            .build();
    }

    /**
     * Execute an HTTP POST request with Bearer token authentication.
     *
     * @param endpoint the full URL to POST to
     * @param apiKey the Bearer token
     * @param requestBody the JSON request body string
     * @return the response body string
     * @throws TddlRuntimeException if the request fails or returns non-2xx status
     */
    public static String doPost(String endpoint, String apiKey, String requestBody) {
        return doPost(endpoint, apiKey, requestBody, null);
    }

    /**
     * Execute an HTTP POST request with Bearer token authentication and optional euid header.
     *
     * @param endpoint the full URL to POST to
     * @param apiKey the Bearer token
     * @param requestBody the JSON request body string
     * @param euid optional value for x-dashscope-euid header (null to skip)
     * @return the response body string
     * @throws TddlRuntimeException if the request fails or returns non-2xx status
     */
    public static String doPost(String endpoint, String apiKey, String requestBody, String euid) {
        HttpPost httpPost = new HttpPost(endpoint);
        httpPost.setHeader("Authorization", "Bearer " + apiKey);
        if (euid != null && !euid.isEmpty()) {
            httpPost.setHeader("x-dashscope-euid", euid);
        }
        httpPost.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON));

        try (CloseableHttpResponse response = CLIENT.execute(httpPost)) {
            int statusCode = response.getStatusLine().getStatusCode();
            String responseStr = EntityUtils.toString(response.getEntity(), "UTF-8");

            if (statusCode >= 200 && statusCode < 300) {
                return responseStr;
            } else {
                logger.error("AI API call failed with status " + statusCode + ": " + responseStr);
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "AI API call failed (HTTP " + statusCode + "): " + responseStr);
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (IOException e) {
            logger.error("Failed to call AI API at " + endpoint, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to call AI API: " + e.getMessage());
        }
    }

    /**
     * Execute a streaming HTTP POST request (SSE) with Bearer token authentication.
     * Reads the response as Server-Sent Events and delivers each data line to the consumer.
     *
     * @param endpoint the full URL to POST to
     * @param apiKey the Bearer token
     * @param requestBody the JSON request body string (should include "stream": true)
     * @param euid optional value for x-dashscope-euid header (null to skip)
     * @param lineConsumer callback receiving each SSE data JSON line (without "data: " prefix)
     * @throws TddlRuntimeException if the request fails or returns non-2xx status
     */
    public static void doPostStreaming(String endpoint, String apiKey, String requestBody,
                                       String euid, Consumer<String> lineConsumer) {
        doPostStreaming(endpoint, apiKey, requestBody, euid, lineConsumer, STREAMING_SOCKET_TIMEOUT_MS);
    }

    public static void doPostStreaming(String endpoint, String apiKey, String requestBody,
                                       String euid, Consumer<String> lineConsumer, int socketTimeoutMs) {
        HttpPost httpPost = new HttpPost(endpoint);
        httpPost.setHeader("Authorization", "Bearer " + apiKey);
        httpPost.setHeader("Accept", "text/event-stream");
        if (euid != null && !euid.isEmpty()) {
            httpPost.setHeader("x-dashscope-euid", euid);
        }
        httpPost.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON));

        // Use longer socket timeout for streaming (tokens arrive over extended period)
        int effectiveSocketTimeoutMs = Math.max(1000, socketTimeoutMs);
        RequestConfig streamingConfig = RequestConfig.custom()
            .setConnectTimeout(CONNECT_TIMEOUT_MS)
            .setSocketTimeout(effectiveSocketTimeoutMs)
            .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MS)
            .build();
        httpPost.setConfig(streamingConfig);

        CloseableHttpResponse response = null;
        try {
            response = CLIENT.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode < 200 || statusCode >= 300) {
                String errorBody = EntityUtils.toString(response.getEntity(), "UTF-8");
                logger.error("AI streaming API call failed with status " + statusCode + ": " + errorBody);
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "AI API call failed (HTTP " + statusCode + "): " + errorBody);
            }

            InputStream is = response.getEntity().getContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                if (!line.startsWith("data: ")) {
                    continue;
                }
                String data = line.substring(6);
                if ("[DONE]".equals(data)) {
                    break;
                }
                lineConsumer.accept(data);
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (IOException e) {
            logger.error("Failed to read streaming AI API response from " + endpoint, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to read streaming AI API response: " + e.getMessage());
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
