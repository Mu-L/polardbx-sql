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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class OpenAiApiProviderTest {

    private MockedStatic<AiHttpClient> mockedHttpClient;
    private OpenAiApiProvider provider;
    private ArgumentCaptor<String> bodyCaptor;

    @Before
    public void setUp() {
        provider = new OpenAiApiProvider();
        bodyCaptor = ArgumentCaptor.forClass(String.class);
        mockedHttpClient = Mockito.mockStatic(AiHttpClient.class);
    }

    @After
    public void tearDown() {
        mockedHttpClient.close();
    }

    private ModelConfigRecord createModelConfig(String modelParams) {
        ModelConfigRecord config = new ModelConfigRecord();
        config.endpoint = "https://api.example.com/v1/chat/completions";
        config.apiKey = "test-key";
        config.model = "gpt-4";
        config.modelParams = modelParams;
        return config;
    }

    // ==================== enable_thinking tests ====================

    @Test
    public void testChatDefaultEnableThinkingFalse() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn("{\"choices\":[{\"message\":{\"content\":\"hello\"}}]}");

        ModelConfigRecord config = createModelConfig(null);
        provider.chatCompletion(config, "test", null);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        assertEquals(false, body.getBoolean("enable_thinking"));
    }

    @Test
    public void testChatEnableThinkingTrueFromOptions() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn("{\"choices\":[{\"message\":{\"content\":\"hello\"}}]}");

        ModelConfigRecord config = createModelConfig(null);
        JSONObject options = new JSONObject();
        options.put("enable_thinking", true);
        provider.chatCompletion(config, "test", options);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        assertEquals(true, body.getBoolean("enable_thinking"));
    }

    @Test
    public void testChatEnableThinkingTrueFromModelParams() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn("{\"choices\":[{\"message\":{\"content\":\"hello\"}}]}");

        ModelConfigRecord config = createModelConfig("{\"enable_thinking\":true}");
        provider.chatCompletion(config, "test", null);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        assertEquals(true, body.getBoolean("enable_thinking"));
    }

    // ==================== option propagation tests ====================

    @Test
    public void testChatModelParamsPropagate() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn("{\"choices\":[{\"message\":{\"content\":\"hello\"}}]}");

        ModelConfigRecord config = createModelConfig(
            "{\"temperature\":0.5,\"top_p\":0.9,\"presence_penalty\":0.6}");
        provider.chatCompletion(config, "test", null);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        assertEquals(0.5, body.getDouble("temperature"), 0.001);
        assertEquals(0.9, body.getDouble("top_p"), 0.001);
        assertEquals(0.6, body.getDouble("presence_penalty"), 0.001);
    }

    @Test
    public void testChatOptionsOverrideModelParams() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn("{\"choices\":[{\"message\":{\"content\":\"hello\"}}]}");

        ModelConfigRecord config = createModelConfig("{\"temperature\":0.5}");
        JSONObject options = new JSONObject();
        options.put("temperature", 0.9);
        provider.chatCompletion(config, "test", options);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        assertEquals(0.9, body.getDouble("temperature"), 0.001);
    }

    @Test
    public void testEmbeddingModelParamsPropagate() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn("{\"data\":[{\"embedding\":[0.1,0.2,0.3]}]}");

        ModelConfigRecord config = createModelConfig("{\"dimensions\":1024,\"encoding_format\":\"float\"}");
        config.endpoint = "https://api.example.com/v1/embeddings";
        provider.embedding(config, "test text", null);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        assertEquals(1024, body.getIntValue("dimensions"));
        assertEquals("float", body.getString("encoding_format"));
    }

    @Test
    public void testRerankModelParamsPropagate() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn("{\"results\":[{\"index\":0,\"relevance_score\":0.95}]}");

        ModelConfigRecord config = createModelConfig("{\"top_n\":5,\"return_documents\":true}");
        config.endpoint = "https://api.example.com/v1/rerank";
        provider.rerank(config, "query", "doc", null);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        assertEquals(5, body.getIntValue("top_n"));
        assertEquals(true, body.getBoolean("return_documents"));
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
    public void testChatRejectsReservedKeyMessagesInOptions() {
        ModelConfigRecord config = createModelConfig(null);
        JSONObject options = new JSONObject();
        options.put("messages", "evil");
        provider.chatCompletion(config, "test", options);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testEmbeddingRejectsReservedKeyInputInOptions() {
        ModelConfigRecord config = createModelConfig(null);
        config.endpoint = "https://api.example.com/v1/embeddings";
        JSONObject options = new JSONObject();
        options.put("input", "evil");
        provider.embedding(config, "test", options);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRerankRejectsReservedKeyDocumentsInModelParams() {
        ModelConfigRecord config = createModelConfig("{\"documents\":[\"evil\"]}");
        config.endpoint = "https://api.example.com/v1/rerank";
        provider.rerank(config, "query", "doc", null);
    }

    // ==================== system_prompt exclusion test ====================

    @Test
    public void testSystemPromptNotInBody() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn("{\"choices\":[{\"message\":{\"content\":\"hello\"}}]}");

        ModelConfigRecord config = createModelConfig("{\"system_prompt\":\"be helpful\"}");
        JSONObject options = new JSONObject();
        options.put("system_prompt", "be concise");
        provider.chatCompletion(config, "test", options);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        assertFalse(body.containsKey("system_prompt"));
    }

    @Test
    public void testSystemPromptUsedInMessages() {
        mockedHttpClient.when(() -> AiHttpClient.doPost(anyString(), anyString(), bodyCaptor.capture(), any()))
            .thenReturn("{\"choices\":[{\"message\":{\"content\":\"hello\"}}]}");

        ModelConfigRecord config = createModelConfig(null);
        JSONObject options = new JSONObject();
        options.put("system_prompt", "be concise");
        provider.chatCompletion(config, "test", options);

        JSONObject body = JSON.parseObject(bodyCaptor.getValue());
        String firstMsgContent = body.getJSONArray("messages")
            .getJSONObject(0).getString("content");
        assertEquals("be concise", firstMsgContent);
    }
}
