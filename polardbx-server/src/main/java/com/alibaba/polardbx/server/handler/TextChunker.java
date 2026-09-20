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

package com.alibaba.polardbx.server.handler;

import java.util.function.Consumer;

/**
 * Buffers incoming LLM tokens and emits text chunks at sentence/clause boundaries.
 * All size thresholds are measured in display width (CJK chars = 2, ASCII = 1)
 * to ensure proper alignment in mysql table format.
 */
public class TextChunker {

    // All thresholds are in display columns (not character count)
    private static final int MIN_CHUNK = 20;
    private static final int CLAUSE_CHUNK = 80;
    private static final int MAX_CHUNK = 100;
    private static final int NEWLINE_FLUSH_MIN = 20;

    private static final String SENTENCE_BOUNDARIES = "\u3002\uff01\uff1f";
    private static final String CLAUSE_BOUNDARIES = "\uff0c\uff1b\u3001\uff1a";

    private final Consumer<String> callback;
    private final StringBuilder buf = new StringBuilder();

    public TextChunker(Consumer<String> callback) {
        this.callback = callback;
    }

    /**
     * Feed a token from the LLM stream. May trigger 0 or more callback invocations.
     * Newlines are treated as hard flush boundaries (excluded from output).
     * Consecutive newlines emit empty chunks so that blank lines are preserved.
     */
    public void feed(String token) {
        if (token == null || token.isEmpty()) {
            return;
        }
        int start = 0;
        for (int i = 0; i < token.length(); i++) {
            if (token.charAt(i) == '\n') {
                if (i > start) {
                    buf.append(token, start, i);
                }
                if (buf.length() == 0) {
                    // Consecutive newline: preserve blank line.
                    callback.accept("");
                } else {
                    // Flush buffer at newline.
                    callback.accept(buf.toString());
                    buf.setLength(0);
                }
                start = i + 1;
            }
        }
        if (start < token.length()) {
            buf.append(token, start, token.length());
        }
        drain();
    }

    /**
     * Force flush any remaining buffered content. Returns the flushed text.
     */
    public String flush() {
        if (buf.length() == 0) {
            return "";
        }
        String remaining = buf.toString().trim();
        buf.setLength(0);
        if (!remaining.isEmpty()) {
            callback.accept(remaining);
        }
        return remaining;
    }

    private void drain() {
        while (displayWidth(buf) >= MIN_CHUNK) {
            int boundary = findBoundary();
            if (boundary > 0) {
                String chunk = buf.substring(0, boundary);
                buf.delete(0, boundary);
                callback.accept(chunk);
            } else if (displayWidth(buf) >= MAX_CHUNK) {
                int pos = findMaxChunkPos();
                String chunk = buf.substring(0, pos);
                buf.delete(0, pos);
                callback.accept(chunk);
            } else {
                break;
            }
        }
    }

    /**
     * Find the best boundary position to split at, using display width thresholds.
     */
    private int findBoundary() {
        int displayW = 0;
        int lastClause = -1;

        for (int i = 0; i < buf.length(); i++) {
            char c = buf.charAt(i);
            displayW += charWidth(c);

            if (displayW >= MIN_CHUNK) {
                if (SENTENCE_BOUNDARIES.indexOf(c) >= 0) {
                    return i + 1;
                }
                if (CLAUSE_BOUNDARIES.indexOf(c) >= 0) {
                    lastClause = i + 1;
                }
            }
        }

        if (lastClause > 0 && displayW >= CLAUSE_CHUNK) {
            return lastClause;
        }

        return -1;
    }

    /**
     * Find character position where display width reaches MAX_CHUNK.
     */
    private int findMaxChunkPos() {
        int w = 0;
        for (int i = 0; i < buf.length(); i++) {
            w += charWidth(buf.charAt(i));
            if (w >= MAX_CHUNK) {
                return i + 1;
            }
        }
        return buf.length();
    }

    private static int displayWidth(StringBuilder sb) {
        int w = 0;
        for (int i = 0; i < sb.length(); i++) {
            w += charWidth(sb.charAt(i));
        }
        return w;
    }

    private static int charWidth(char c) {
        if (c >= 0x4E00 && c <= 0x9FFF) {
            return 2;  // CJK Unified Ideographs
        }
        if (c >= 0x3000 && c <= 0x303F) {
            return 2;  // CJK Symbols and Punctuation
        }
        if (c >= 0xFF00 && c <= 0xFFEF) {
            return 2;  // Fullwidth Forms
        }
        if (c >= 0x3400 && c <= 0x4DBF) {
            return 2;  // CJK Extension A
        }
        if (c >= 0x2E80 && c <= 0x2FFF) {
            return 2;  // CJK Radicals
        }
        if (c >= 0xF900 && c <= 0xFAFF) {
            return 2;  // CJK Compatibility Ideographs
        }
        if (c >= 0xFE30 && c <= 0xFE4F) {
            return 2;  // CJK Compatibility Forms
        }
        return 1;
    }
}
