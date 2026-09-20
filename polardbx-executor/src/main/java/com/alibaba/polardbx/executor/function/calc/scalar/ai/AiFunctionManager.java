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

package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.optimizer.core.function.calc.IScalarFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Registry for all AI inference functions.
 *
 * <p>Each registered function must implement both {@link IScalarFunction}
 * and {@link AiFunctionMetadata}. The registry is used by
 * {@code SHOW AI FUNCTION} to enumerate available AI functions.
 */
public final class AiFunctionManager {

    private static final List<IScalarFunction> REGISTRY = new ArrayList<>();

    static {
        REGISTRY.add(new AiPromptFunction());
        REGISTRY.add(new AiEmbeddingFunction());
        REGISTRY.add(new AiClassifyFunction());
        REGISTRY.add(new AiRankFunction());
        REGISTRY.add(new AiParseDocumentFunction());
        REGISTRY.add(new AiVlEmbeddingFunction());
        REGISTRY.add(new AiSimilarityFunction());
        REGISTRY.add(new AiExtractFunction());
        REGISTRY.add(new AiSummarizeFunction());
        REGISTRY.add(new AiText2SqlFunction());
    }

    private AiFunctionManager() {
    }

    /**
     * Get all registered AI function instances sorted by function name.
     *
     * @return unmodifiable list of AI function instances
     */
    public static List<IScalarFunction> getRegisteredFunctions() {
        List<IScalarFunction> sorted = new ArrayList<>(REGISTRY);
        sorted.sort((a, b) -> a.getFunctionNames()[0].compareTo(b.getFunctionNames()[0]));
        return Collections.unmodifiableList(sorted);
    }
}
