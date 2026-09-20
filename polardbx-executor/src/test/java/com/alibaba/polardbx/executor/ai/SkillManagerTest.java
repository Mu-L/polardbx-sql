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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.metadb.model.SkillConfigRecord;
import com.alibaba.polardbx.gms.metadb.model.SkillReferenceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Unit tests for {@link SkillManager}.
 * Focuses on pure-logic methods: file parsing, cache read operations,
 * and input validation of write operations.
 */
public class SkillManagerTest {

    // ==================== Helpers ====================

    private SkillManager createUninitializedInstance() throws Exception {
        Constructor<SkillManager> ctor = SkillManager.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        return ctor.newInstance();
    }

    private void injectCache(SkillManager sm, Map<String, SkillConfigRecord> skillCache,
                             Map<String, List<SkillReferenceRecord>> refCache) throws Exception {
        Field scField = SkillManager.class.getDeclaredField("skillCache");
        scField.setAccessible(true);
        scField.set(sm, skillCache);

        Field rcField = SkillManager.class.getDeclaredField("refCache");
        rcField.setAccessible(true);
        rcField.set(sm, refCache);
    }

    private String[] invokeParseSkillFile(SkillManager sm, String raw) throws Exception {
        Method m = SkillManager.class.getDeclaredMethod("parseSkillFile", String.class);
        m.setAccessible(true);
        return (String[]) m.invoke(sm, raw);
    }

    private String invokeParseFrontmatterDescription(SkillManager sm, String frontmatter) throws Exception {
        Method m = SkillManager.class.getDeclaredMethod("parseFrontmatterDescription", String.class);
        m.setAccessible(true);
        return (String) m.invoke(sm, frontmatter);
    }

    @SuppressWarnings("unchecked")
    private List<String> invokeParseManifest(SkillManager sm, String manifest) throws Exception {
        Method m = SkillManager.class.getDeclaredMethod("parseManifest", String.class);
        m.setAccessible(true);
        return (List<String>) m.invoke(sm, manifest);
    }

    private SkillConfigRecord createSkill(String name, String status, int priority, int builtin, long id) {
        SkillConfigRecord r = new SkillConfigRecord();
        r.name = name;
        r.status = status;
        r.priority = priority;
        r.builtin = builtin;
        r.id = id;
        r.description = "desc-" + name;
        r.prompt = "prompt-" + name;
        return r;
    }

    private SkillReferenceRecord createRef(String skillName, String refName, String content, int priority) {
        SkillReferenceRecord r = new SkillReferenceRecord();
        r.skillName = skillName;
        r.refName = refName;
        r.content = content;
        r.priority = priority;
        return r;
    }

    // ==================== parseSkillFile ====================

    @Test
    public void testParseSkillFile_noFrontmatter() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String raw = "This is the prompt body.\nSecond line.";
        String[] result = invokeParseSkillFile(sm, raw);
        Assert.assertEquals("", result[0]);
        Assert.assertEquals("This is the prompt body.\nSecond line.", result[1]);
    }

    @Test
    public void testParseSkillFile_withFrontmatter() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String raw = "---\ndescription: My desc\n---\nPrompt body here.";
        String[] result = invokeParseSkillFile(sm, raw);
        Assert.assertEquals("My desc", result[0]);
        Assert.assertEquals("Prompt body here.", result[1]);
    }

    @Test
    public void testParseSkillFile_frontmatterCrLf() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String raw = "---\r\ndescription: My desc\r\n---\r\nPrompt body.";
        String[] result = invokeParseSkillFile(sm, raw);
        Assert.assertEquals("My desc", result[0]);
        Assert.assertEquals("Prompt body.", result[1]);
    }

    @Test
    public void testParseSkillFile_frontmatterNoClosing() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String raw = "---\ndescription: My desc\nPrompt body.";
        String[] result = invokeParseSkillFile(sm, raw);
        // No closing ---, treated as no frontmatter
        Assert.assertEquals("", result[0]);
        Assert.assertTrue(result[1].contains("description:"));
    }

    @Test
    public void testParseSkillFile_descriptionTruncated() throws Exception {
        SkillManager sm = createUninitializedInstance();
        StringBuilder longDesc = new StringBuilder();
        for (int i = 0; i < 1010; i++) {
            longDesc.append('a');
        }
        String raw = "---\ndescription: " + longDesc.toString() + "\n---\nPrompt.";
        String[] result = invokeParseSkillFile(sm, raw);
        Assert.assertEquals(1000, result[0].length());
    }

    // ==================== parseFrontmatterDescription ====================

    @Test
    public void testParseFrontmatterDescription_singleLine() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String fm = "description: Hello world\nother: value";
        Assert.assertEquals("Hello world", invokeParseFrontmatterDescription(sm, fm));
    }

    @Test
    public void testParseFrontmatterDescription_singleLineEmpty() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String fm = "description:\nother: value";
        Assert.assertEquals("", invokeParseFrontmatterDescription(sm, fm));
    }

    @Test
    public void testParseFrontmatterDescription_blockScalar() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String fm = "description: |\n  Line one\n  Line two\nother: value";
        Assert.assertEquals("Line one\nLine two", invokeParseFrontmatterDescription(sm, fm));
    }

    @Test
    public void testParseFrontmatterDescription_blockScalarStrip() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String fm = "description: |-\n  Line one\n  Line two\nother: value";
        Assert.assertEquals("Line one\nLine two", invokeParseFrontmatterDescription(sm, fm));
    }

    @Test
    public void testParseFrontmatterDescription_blockScalarKeep() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String fm = "description: |+\n  Line one\n  Line two\nother: value";
        Assert.assertEquals("Line one\nLine two", invokeParseFrontmatterDescription(sm, fm));
    }

    @Test
    public void testParseFrontmatterDescription_blockScalarWithBlankLines() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String fm = "description: |\n  Line one\n\n  Line two\nother: value";
        Assert.assertEquals("Line one\n\nLine two", invokeParseFrontmatterDescription(sm, fm));
    }

    @Test
    public void testParseFrontmatterDescription_noDescription() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String fm = "name: test\nother: value";
        Assert.assertEquals("", invokeParseFrontmatterDescription(sm, fm));
    }

    @Test
    public void testParseFrontmatterDescription_noIndentBreak() throws Exception {
        SkillManager sm = createUninitializedInstance();
        String fm = "description: |\n  Line one\nno indent\nother: value";
        Assert.assertEquals("Line one", invokeParseFrontmatterDescription(sm, fm));
    }

    // ==================== parseManifest ====================

    @Test
    public void testParseManifest_basic() throws Exception {
        SkillManager sm = createUninitializedInstance();
        List<String> files = invokeParseManifest(sm, "ref1.md\nref2.md");
        Assert.assertEquals(2, files.size());
        Assert.assertEquals("ref1.md", files.get(0));
        Assert.assertEquals("ref2.md", files.get(1));
    }

    @Test
    public void testParseManifest_skipsEmptyAndComments() throws Exception {
        SkillManager sm = createUninitializedInstance();
        List<String> files = invokeParseManifest(sm, "ref1.md\n\n  \n# comment\nref2.md");
        Assert.assertEquals(2, files.size());
        Assert.assertEquals("ref1.md", files.get(0));
        Assert.assertEquals("ref2.md", files.get(1));
    }

    @Test
    public void testParseManifest_empty() throws Exception {
        SkillManager sm = createUninitializedInstance();
        List<String> files = invokeParseManifest(sm, "");
        Assert.assertTrue(files.isEmpty());
    }

    @Test
    public void testParseManifest_allComments() throws Exception {
        SkillManager sm = createUninitializedInstance();
        List<String> files = invokeParseManifest(sm, "# comment1\n# comment2");
        Assert.assertTrue(files.isEmpty());
    }

    // ==================== getActiveSkills ====================

    @Test
    public void testGetActiveSkills() throws Exception {
        SkillManager sm = createUninitializedInstance();
        Map<String, SkillConfigRecord> sc = new ConcurrentHashMap<>();
        sc.put("s1", createSkill("s1", "ACTIVE", 10, 0, 1));
        sc.put("s2", createSkill("s2", "INACTIVE", 20, 0, 2));
        sc.put("s3", createSkill("s3", "ACTIVE", 5, 0, 3));
        injectCache(sm, sc, new ConcurrentHashMap<>());

        List<SkillConfigRecord> active = sm.getActiveSkills();
        Assert.assertEquals(2, active.size());
        // Sorted by priority asc, then id asc
        Assert.assertEquals("s3", active.get(0).name);
        Assert.assertEquals("s1", active.get(1).name);
    }

    @Test
    public void testGetActiveSkills_empty() throws Exception {
        SkillManager sm = createUninitializedInstance();
        injectCache(sm, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        Assert.assertTrue(sm.getActiveSkills().isEmpty());
    }

    // ==================== getSkillConfig ====================

    @Test
    public void testGetSkillConfig_found() throws Exception {
        SkillManager sm = createUninitializedInstance();
        Map<String, SkillConfigRecord> sc = new ConcurrentHashMap<>();
        sc.put("s1", createSkill("s1", "ACTIVE", 10, 0, 1));
        injectCache(sm, sc, new ConcurrentHashMap<>());

        SkillConfigRecord r = sm.getSkillConfig("s1");
        Assert.assertNotNull(r);
        Assert.assertEquals("s1", r.name);
    }

    @Test
    public void testGetSkillConfig_notFound() throws Exception {
        SkillManager sm = createUninitializedInstance();
        injectCache(sm, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        Assert.assertNull(sm.getSkillConfig("missing"));
    }

    @Test
    public void testGetSkillConfig_inactive() throws Exception {
        SkillManager sm = createUninitializedInstance();
        Map<String, SkillConfigRecord> sc = new ConcurrentHashMap<>();
        sc.put("s1", createSkill("s1", "INACTIVE", 10, 0, 1));
        injectCache(sm, sc, new ConcurrentHashMap<>());
        Assert.assertNull(sm.getSkillConfig("s1"));
    }

    // ==================== getReferences ====================

    @Test
    public void testGetReferences_found() throws Exception {
        SkillManager sm = createUninitializedInstance();
        Map<String, List<SkillReferenceRecord>> rc = new ConcurrentHashMap<>();
        rc.put("s1", java.util.Arrays.asList(createRef("s1", "r1", "content1", 10)));
        injectCache(sm, new ConcurrentHashMap<>(), rc);

        List<SkillReferenceRecord> refs = sm.getReferences("s1");
        Assert.assertEquals(1, refs.size());
        Assert.assertEquals("r1", refs.get(0).refName);
    }

    @Test
    public void testGetReferences_notFound() throws Exception {
        SkillManager sm = createUninitializedInstance();
        injectCache(sm, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        List<SkillReferenceRecord> refs = sm.getReferences("missing");
        Assert.assertTrue(refs.isEmpty());
    }

    // ==================== listSkills ====================

    @Test
    public void testListSkills() throws Exception {
        SkillManager sm = createUninitializedInstance();
        Map<String, SkillConfigRecord> sc = new ConcurrentHashMap<>();
        sc.put("s1", createSkill("s1", "ACTIVE", 10, 0, 1));
        sc.put("s2", createSkill("s2", "ACTIVE", 5, 1, 2));

        Map<String, List<SkillReferenceRecord>> rc = new ConcurrentHashMap<>();
        rc.put("s1", java.util.Arrays.asList(createRef("s1", "r1", "c1", 10)));
        injectCache(sm, sc, rc);

        String json = sm.listSkills();
        JSONArray arr = JSONArray.parseArray(json);
        Assert.assertEquals(2, arr.size());

        // Sorted by priority: s2 first (priority 5)
        JSONObject first = arr.getJSONObject(0);
        Assert.assertEquals("s2", first.getString("name"));
        Assert.assertTrue(first.getBoolean("builtin"));

        JSONObject second = arr.getJSONObject(1);
        Assert.assertEquals("s1", second.getString("name"));
        Assert.assertFalse(second.getBoolean("builtin"));
        JSONArray refNames = second.getJSONArray("references");
        Assert.assertEquals(1, refNames.size());
        Assert.assertEquals("r1", refNames.getString(0));
    }

    @Test
    public void testListSkills_empty() throws Exception {
        SkillManager sm = createUninitializedInstance();
        injectCache(sm, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        String json = sm.listSkills();
        Assert.assertEquals("[]", json);
    }

    // ==================== describeSkill ====================

    @Test
    public void testDescribeSkill_found() throws Exception {
        SkillManager sm = createUninitializedInstance();
        Map<String, SkillConfigRecord> sc = new ConcurrentHashMap<>();
        sc.put("s1", createSkill("s1", "ACTIVE", 10, 0, 1));

        Map<String, List<SkillReferenceRecord>> rc = new ConcurrentHashMap<>();
        rc.put("s1", java.util.Arrays.asList(
            createRef("s1", "r1", "content1", 10),
            createRef("s1", "r2", null, 20)
        ));
        injectCache(sm, sc, rc);

        String json = sm.describeSkill("s1");
        JSONObject obj = JSONObject.parseObject(json);
        Assert.assertEquals("s1", obj.getString("name"));
        Assert.assertFalse(obj.getBoolean("builtin"));
        JSONArray refs = obj.getJSONArray("references");
        Assert.assertEquals(2, refs.size());
        Assert.assertEquals(8, refs.getJSONObject(0).getIntValue("content_length"));
        Assert.assertEquals(0, refs.getJSONObject(1).getIntValue("content_length"));
    }

    @Test(expected = TddlRuntimeException.class)
    public void testDescribeSkill_notFound() throws Exception {
        SkillManager sm = createUninitializedInstance();
        injectCache(sm, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        sm.describeSkill("missing");
    }

    // ==================== describeSkill no refs ====================

    @Test
    public void testDescribeSkill_noRefs() throws Exception {
        SkillManager sm = createUninitializedInstance();
        Map<String, SkillConfigRecord> sc = new ConcurrentHashMap<>();
        sc.put("s1", createSkill("s1", "ACTIVE", 10, 1, 1));
        injectCache(sm, sc, new ConcurrentHashMap<>());

        String json = sm.describeSkill("s1");
        JSONObject obj = JSONObject.parseObject(json);
        Assert.assertTrue(obj.getBoolean("builtin"));
        Assert.assertFalse(obj.containsKey("references"));
    }

    // ==================== registerSkill validation ====================

    @Test(expected = TddlRuntimeException.class)
    public void testRegisterSkill_nullName() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.registerSkill(null, "prompt", null);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRegisterSkill_emptyName() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.registerSkill("  ", "prompt", null);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRegisterSkill_nullPrompt() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.registerSkill("name", null, null);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRegisterSkill_emptyPrompt() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.registerSkill("name", "  ", null);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRegisterSkill_builtinConflict() throws Exception {
        SkillManager sm = createUninitializedInstance();
        Map<String, SkillConfigRecord> sc = new ConcurrentHashMap<>();
        SkillConfigRecord builtin = createSkill("builtin-skill", "ACTIVE", 10, 1, 1);
        sc.put("builtin-skill", builtin);
        injectCache(sm, sc, new ConcurrentHashMap<>());
        sm.registerSkill("builtin-skill", "prompt", null);
    }

    // ==================== updateSkill validation ====================

    @Test(expected = TddlRuntimeException.class)
    public void testUpdateSkill_nullName() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.updateSkill(null, "{}");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testUpdateSkill_emptyOptions() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.updateSkill("name", null);
    }

    // ==================== dropSkill validation ====================

    @Test(expected = TddlRuntimeException.class)
    public void testDropSkill_nullName() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.dropSkill(null);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testDropSkill_builtin() throws Exception {
        SkillManager sm = createUninitializedInstance();
        Map<String, SkillConfigRecord> sc = new ConcurrentHashMap<>();
        sc.put("builtin", createSkill("builtin", "ACTIVE", 10, 1, 1));
        injectCache(sm, sc, new ConcurrentHashMap<>());
        sm.dropSkill("builtin");
    }

    // ==================== addReference validation ====================

    @Test(expected = TddlRuntimeException.class)
    public void testAddReference_nullSkillName() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.addReference(null, "ref", "content");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testAddReference_nullRefName() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.addReference("skill", null, "content");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testAddReference_nullContent() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.addReference("skill", "ref", null);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testAddReference_emptyContent() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.addReference("skill", "ref", "   ");
    }

    // ==================== removeReference validation ====================

    @Test(expected = TddlRuntimeException.class)
    public void testRemoveReference_nullSkillName() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.removeReference(null, "ref");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRemoveReference_nullRefName() throws Exception {
        SkillManager sm = createUninitializedInstance();
        sm.removeReference("skill", null);
    }
}
