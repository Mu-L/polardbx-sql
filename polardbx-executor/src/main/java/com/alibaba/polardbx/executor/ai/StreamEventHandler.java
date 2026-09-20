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

/**
 * Callback interface for handling streaming SSE events from LLM API.
 * Used by OpenAiApiProvider.chatCompletionWithToolsStreaming().
 */
public interface StreamEventHandler {

    /**
     * Called when a content token delta is received.
     *
     * @param text the content text fragment
     */
    void onContentDelta(String text);

    /**
     * Called when a tool_call delta is received.
     *
     * @param index the tool call index (for parallel tool calls)
     * @param id the tool call ID (only present in the first chunk for this index)
     * @param functionName the function name (only present in the first chunk)
     * @param argsDelta incremental arguments JSON fragment
     */
    void onToolCallDelta(int index, String id, String functionName, String argsDelta);

    /**
     * Called when the stream completes successfully.
     *
     * @param finishReason the finish reason ("stop", "tool_calls", etc.)
     */
    void onComplete(String finishReason);

    /**
     * Called when an error occurs during streaming.
     *
     * @param e the exception
     */
    void onError(Exception e);
}
