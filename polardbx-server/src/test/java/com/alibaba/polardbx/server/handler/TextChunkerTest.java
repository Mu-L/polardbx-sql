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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link TextChunker}.
 * Covers feed, flush, sentence/clause boundaries, CJK display width,
 * newline handling, and max-chunk forced splitting.
 */
public class TextChunkerTest {

    private List<String> collectChunks(String input) {
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        chunker.feed(input);
        chunker.flush();
        return chunks;
    }

    private List<String> collectChunksFeedSeparately(String... tokens) {
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        for (String token : tokens) {
            chunker.feed(token);
        }
        chunker.flush();
        return chunks;
    }

    // ==================== Basic feed/flush ====================

    @Test
    public void testFeed_nullToken() {
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        chunker.feed(null);
        chunker.flush();
        Assert.assertTrue(chunks.isEmpty());
    }

    @Test
    public void testFeed_emptyToken() {
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        chunker.feed("");
        chunker.flush();
        Assert.assertTrue(chunks.isEmpty());
    }

    @Test
    public void testFlush_emptyBuffer() {
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        String result = chunker.flush();
        Assert.assertEquals("", result);
        Assert.assertTrue(chunks.isEmpty());
    }

    @Test
    public void testFeed_shortText_noChunkUntilFlush() {
        // Text shorter than MIN_CHUNK (20 display width) stays buffered
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        chunker.feed("Hello");
        Assert.assertTrue(chunks.isEmpty()); // not enough to emit
        String remaining = chunker.flush();
        Assert.assertEquals("Hello", remaining);
        Assert.assertEquals(1, chunks.size());
        Assert.assertEquals("Hello", chunks.get(0));
    }

    // ==================== Sentence boundary splitting ====================

    @Test
    public void testFeed_sentenceBoundary_chinese() {
        // Chinese period (。) is a sentence boundary
        String input = "这是第一句话。这是第二句话。这是第三句话。";
        List<String> chunks = collectChunks(input);
        // Should split at sentence boundaries after MIN_CHUNK display width
        Assert.assertTrue(chunks.size() >= 2);
    }

    @Test
    public void testFeed_exclamationMark_boundary() {
        // ！ is a sentence boundary
        String input = "注意！这是一条重要信息！请仔细阅读！这是额外内容。";
        List<String> chunks = collectChunks(input);
        Assert.assertTrue(chunks.size() >= 2);
    }

    @Test
    public void testFeed_questionMark_boundary() {
        // ？ is a sentence boundary
        String input = "这是什么？这是数据库。你怎么看？我觉得不错。";
        List<String> chunks = collectChunks(input);
        Assert.assertTrue(chunks.size() >= 2);
    }

    // ==================== Clause boundary splitting ====================

    @Test
    public void testFeed_clauseBoundary_chinese() {
        // ，(comma) is a clause boundary. The findBoundary() logic records the last
        // clause position and returns it when total displayW >= CLAUSE_CHUNK (80).
        // Use a mix of clause and sentence boundaries to force multiple splits.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            sb.append("这是一个较长的子句，");
        }
        sb.append("这是最后一句。"); // sentence boundary triggers immediate split
        for (int i = 0; i < 5; i++) {
            sb.append("这是另一段子句，");
        }
        sb.append("最终总结。"); // sentence boundary
        List<String> chunks = collectChunks(sb.toString());
        Assert.assertTrue("Expected >= 2 chunks but got " + chunks.size(), chunks.size() >= 2);
    }

    @Test
    public void testFeed_semicolon_boundary() {
        // ；is a clause boundary
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append("条件一满足；");
        }
        List<String> chunks = collectChunks(sb.toString());
        Assert.assertTrue(chunks.size() >= 1);
    }

    // ==================== CJK display width ====================

    @Test
    public void testFeed_cjkCharsWider() {
        // CJK chars = 2 display width each. 10 CJK chars = 20 display width = MIN_CHUNK
        // With sentence boundary, should split
        String input = "一二三四五六七八九十。后面还有内容。";
        List<String> chunks = collectChunks(input);
        Assert.assertFalse(chunks.isEmpty());
    }

    @Test
    public void testFeed_mixedCjkAscii() {
        // Mix of CJK and ASCII — display width should be calculated correctly
        String input = "Hello世界，这是测试内容。继续添加更多文字来达到分块阈值。";
        List<String> chunks = collectChunks(input);
        Assert.assertFalse(chunks.isEmpty());
    }

    // ==================== Newline handling ====================

    @Test
    public void testFeed_newlineFlush() {
        // Newline triggers flush if buffer has enough content
        String input = "This is a line with enough content\nSecond line here\n";
        List<String> chunks = collectChunks(input);
        Assert.assertTrue(chunks.size() >= 1);
        // Newlines should not appear in output
        for (String chunk : chunks) {
            Assert.assertFalse(chunk.contains("\n"));
        }
    }

    @Test
    public void testFeed_newlineShortBuffer() {
        // Newline always flushes the buffered text, even if it is short.
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        chunker.feed("Hi\nWorld and more text to reach the minimum chunk threshold");
        chunker.flush();
        Assert.assertFalse(chunks.isEmpty());
        Assert.assertEquals("Hi", chunks.get(0));
    }

    @Test
    public void testFeed_multipleNewlines() {
        String input = "First paragraph with enough text\n\nSecond paragraph with enough text\n";
        List<String> chunks = collectChunks(input);
        Assert.assertTrue(chunks.size() >= 3);
        // The blank line between the two paragraphs should be preserved as an empty chunk.
        boolean hasBlank = false;
        for (String chunk : chunks) {
            if (chunk.isEmpty()) {
                hasBlank = true;
                break;
            }
        }
        Assert.assertTrue(hasBlank);
    }

    // ==================== Progressive feeding ====================

    @Test
    public void testFeed_multipleTokens() {
        // Feed tokens one by one — same result as feeding all at once
        List<String> chunks = collectChunksFeedSeparately(
            "这是一", "个测试", "。", "第二部分内容", "。");
        Assert.assertTrue(chunks.size() >= 1);
    }

    // ==================== Max chunk forced split ====================

    @Test
    public void testFeed_maxChunkForcedSplit() {
        // Very long text without any boundary should be force-split at MAX_CHUNK (100 display width)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb.append("a");
        }
        List<String> chunks = collectChunks(sb.toString());
        Assert.assertTrue(chunks.size() >= 2);
        // Each chunk should not exceed MAX_CHUNK display width
        for (String chunk : chunks) {
            Assert.assertTrue(chunk.length() <= 101); // some tolerance for boundary
        }
    }

    // ==================== Flush behavior ====================

    @Test
    public void testFlush_returnsRemainingContent() {
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        chunker.feed("Some text");
        String remaining = chunker.flush();
        Assert.assertEquals("Some text", remaining);
    }

    @Test
    public void testFlush_calledTwice() {
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        chunker.feed("Hello");
        chunker.flush();
        String second = chunker.flush();
        Assert.assertEquals("", second);
    }

    // ==================== Fullwidth characters ====================

    @Test
    public void testFeed_fullwidthChars() {
        // Fullwidth forms (FF00-FFEF) = 2 display width each
        String input = "ＡＢＣＤＥＦＧＨＩＪ。后续内容。";
        List<String> chunks = collectChunks(input);
        Assert.assertFalse(chunks.isEmpty());
    }

    // ==================== Edge cases ====================

    @Test
    public void testFeed_onlyNewlines() {
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        chunker.feed("\n\n\n");
        chunker.flush();
        // Consecutive newlines emit empty chunks to preserve blank lines.
        Assert.assertEquals(3, chunks.size());
        for (String chunk : chunks) {
            Assert.assertEquals("", chunk);
        }
    }

    @Test
    public void testFeed_trailingNewline() {
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        chunker.feed("Some text that is long enough to flush at newline\n");
        Assert.assertFalse(chunks.isEmpty());
    }

    @Test
    public void testFeed_whitespaceOnlyAfterTrim() {
        List<String> chunks = new ArrayList<>();
        TextChunker chunker = new TextChunker(chunks::add);
        chunker.feed("   \n   \n   ");
        String remaining = chunker.flush();
        // Newlines flush whitespace chunks; flush() trims trailing whitespace.
        Assert.assertTrue(remaining.isEmpty() || remaining.trim().isEmpty());
        Assert.assertFalse(chunks.isEmpty());
    }
}
