package com.alibaba.polardbx.executor.ai;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class DashScopeApiProviderTest {

    private MockedStatic<AiHttpClient> mockedHttpClient;
    private DashScopeApiProvider provider;
    private ArgumentCaptor<String> bodyCaptor;

    @Before
    public void setUp() {
        provider = new DashScopeApiProvider();
        bodyCaptor = ArgumentCaptor.forClass(String.class);
        mockedHttpClient = Mockito.mockStatic(AiHttpClient.class);
    }

    @After
    public void tearDown() {
        mockedHttpClient.close();
    }

    private ModelConfigRecord createModelConfig(String modelParams) {
        ModelConfigRecord config = new ModelConfigRecord();
        config.endpoint = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation";
        config.apiKey = "test-key";
        config.model = "qwen-plus";
        config.modelParams = modelParams;
        return config;
    }

    private static final String CHAT_RESPONSE = "{\"output\":{\"choices\":[{\"message\":{\"content\":\"hello\"}}]}}";
    private static final String EMBEDDING_RESPONSE =
        "{\"output\":{\"embeddings\":[{\"embedding\":[0.1,0.2,0.3]}]}}";
    private static final String RERANK_RESPONSE =
        "{\"output\":{\"results\":[{\"index\":0,\"relevance_score\":0.95}]}}";

    // ==================== chat: enable_thinking tests ====================

    @Test
    public void testChatDefaultEnableThinkingFalse() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn(CHAT_RESPONSE);

        ModelConfigRecord config = createModelConfig(null);
        provider.chatCompletion(config, "test", null);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        JSONObject parameters = body.getJSONObject("parameters");
        assertEquals(false, parameters.getBoolean("enable_thinking"));
    }

    @Test
    public void testChatEnableThinkingTrueFromOptions() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn(CHAT_RESPONSE);

        ModelConfigRecord config = createModelConfig(null);
        JSONObject options = new JSONObject();
        options.put("enable_thinking", true);
        provider.chatCompletion(config, "test", options);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        JSONObject parameters = body.getJSONObject("parameters");
        assertEquals(true, parameters.getBoolean("enable_thinking"));
    }

    // ==================== chat: option propagation tests ====================

    @Test
    public void testChatModelParamsPropagateIntoParameters() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn(CHAT_RESPONSE);

        ModelConfigRecord config = createModelConfig(
            "{\"temperature\":0.5,\"top_p\":0.9,\"repetition_penalty\":1.1}");
        provider.chatCompletion(config, "test", null);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        JSONObject parameters = body.getJSONObject("parameters");
        assertEquals(0.5, parameters.getDouble("temperature"), 0.001);
        assertEquals(0.9, parameters.getDouble("top_p"), 0.001);
        assertEquals(1.1, parameters.getDouble("repetition_penalty"), 0.001);
    }

    @Test
    public void testChatOptionsOverrideModelParams() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn(CHAT_RESPONSE);

        ModelConfigRecord config = createModelConfig("{\"temperature\":0.5}");
        JSONObject options = new JSONObject();
        options.put("temperature", 0.9);
        provider.chatCompletion(config, "test", options);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        JSONObject parameters = body.getJSONObject("parameters");
        assertEquals(0.9, parameters.getDouble("temperature"), 0.001);
    }

    // ==================== embedding: option propagation tests ====================

    @Test
    public void testEmbeddingModelParamsPropagateIntoParameters() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn(EMBEDDING_RESPONSE);

        ModelConfigRecord config = createModelConfig("{\"dimension\":1024,\"output_type\":\"dense\"}");
        config.endpoint = "https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding";
        provider.embedding(config, "test text", null);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        JSONObject parameters = body.getJSONObject("parameters");
        assertEquals(1024, parameters.getIntValue("dimension"));
        assertEquals("dense", parameters.getString("output_type"));
    }

    @Test
    public void testEmbeddingOptionsOverrideModelParams() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn(EMBEDDING_RESPONSE);

        ModelConfigRecord config = createModelConfig("{\"dimension\":512}");
        config.endpoint = "https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding";
        JSONObject options = new JSONObject();
        options.put("dimension", 1024);
        provider.embedding(config, "test text", options);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        JSONObject parameters = body.getJSONObject("parameters");
        assertEquals(1024, parameters.getIntValue("dimension"));
    }

    // ==================== rerank: option propagation tests ====================

    @Test
    public void testRerankModelParamsPropagateIntoParameters() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn(RERANK_RESPONSE);

        ModelConfigRecord config = createModelConfig("{\"top_n\":5,\"return_documents\":true}");
        config.endpoint = "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank";
        provider.rerank(config, "query", "doc", null);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        JSONObject parameters = body.getJSONObject("parameters");
        assertEquals(5, parameters.getIntValue("top_n"));
        assertEquals(true, parameters.getBoolean("return_documents"));
    }

    @Test
    public void testRerankOptionsOverrideDefaults() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn(RERANK_RESPONSE);

        ModelConfigRecord config = createModelConfig(null);
        config.endpoint = "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank";
        JSONObject options = new JSONObject();
        options.put("top_n", 3);
        provider.rerank(config, "query", "doc", options);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        JSONObject parameters = body.getJSONObject("parameters");
        assertEquals(3, parameters.getIntValue("top_n"));
    }

    // ==================== reserved key protection tests ====================

    @Test(expected = TddlRuntimeException.class)
    public void testChatRejectsReservedKeyModelInOptions() {
        ModelConfigRecord config = createModelConfig(null);
        JSONObject options = new JSONObject();
        options.put("model", "evil-model");
        provider.chatCompletion(config, "test", options);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testChatRejectsReservedKeyInputInModelParams() {
        ModelConfigRecord config = createModelConfig("{\"input\":{\"evil\":true}}");
        provider.chatCompletion(config, "test", null);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testEmbeddingRejectsReservedKeyModelInOptions() {
        ModelConfigRecord config = createModelConfig(null);
        config.endpoint = "https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding";
        JSONObject options = new JSONObject();
        options.put("model", "evil");
        provider.embedding(config, "test", options);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRerankRejectsReservedKeyInputInOptions() {
        ModelConfigRecord config = createModelConfig(null);
        config.endpoint = "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank";
        JSONObject options = new JSONObject();
        options.put("input", "evil");
        provider.rerank(config, "query", "doc", options);
    }

    // ==================== system_prompt exclusion test ====================

    @Test
    public void testSystemPromptNotInParameters() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn(CHAT_RESPONSE);

        ModelConfigRecord config = createModelConfig("{\"system_prompt\":\"be helpful\"}");
        JSONObject options = new JSONObject();
        options.put("system_prompt", "be concise");
        provider.chatCompletion(config, "test", options);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        JSONObject parameters = body.getJSONObject("parameters");
        assertFalse(parameters.containsKey("system_prompt"));
        assertFalse(body.containsKey("system_prompt"));
    }
}
