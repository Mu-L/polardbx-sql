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

import com.alibaba.polardbx.executor.ai.SkillManager;
import com.alibaba.polardbx.gms.metadb.model.SkillConfigRecord;
import com.alibaba.polardbx.gms.metadb.model.SkillReferenceRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

public class AiFunctionTest {

    // ==================== AiListSkillsFunction ====================

    @Test
    public void testAiListSkillsFunction() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);
            Mockito.when(sm.listSkills()).thenReturn("[\"s1\"]");

            AiListSkillsFunction fn = new AiListSkillsFunction();
            Object result = fn.compute(new Object[] {}, null);
            Assert.assertEquals("[\"s1\"]", result);
        }
    }

    @Test
    public void testAiListSkillsFunction_names() {
        AiListSkillsFunction fn = new AiListSkillsFunction();
        Assert.assertArrayEquals(new String[] {"AI_LIST_SKILLS"}, fn.getFunctionNames());
    }

    // ==================== AiDescribeSkillFunction ====================

    @Test
    public void testAiDescribeSkillFunction() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);
            Mockito.when(sm.describeSkill("s1")).thenReturn("desc");

            AiDescribeSkillFunction fn = new AiDescribeSkillFunction();
            Object result = fn.compute(new Object[] {"s1"}, null);
            Assert.assertEquals("desc", result);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAiDescribeSkillFunction_noArgs() {
        AiDescribeSkillFunction fn = new AiDescribeSkillFunction();
        fn.compute(new Object[] {}, null);
    }

    // ==================== AiDropSkillFunction ====================

    @Test
    public void testAiDropSkillFunction() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);
            Mockito.when(sm.dropSkill("s1")).thenReturn("dropped");

            AiDropSkillFunction fn = new AiDropSkillFunction();
            Object result = fn.compute(new Object[] {"s1"}, null);
            Assert.assertEquals("dropped", result);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAiDropSkillFunction_noArgs() {
        AiDropSkillFunction fn = new AiDropSkillFunction();
        fn.compute(new Object[] {}, null);
    }

    // ==================== AiRegisterSkillFunction ====================

    @Test
    public void testAiRegisterSkillFunction_twoArgs() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);
            Mockito.when(sm.registerSkill("name", "prompt", null)).thenReturn("ok");

            AiRegisterSkillFunction fn = new AiRegisterSkillFunction();
            Object result = fn.compute(new Object[] {"name", "prompt"}, null);
            Assert.assertEquals("ok", result);
        }
    }

    @Test
    public void testAiRegisterSkillFunction_threeArgs() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);
            Mockito.when(sm.registerSkill("name", "prompt", "{}")).thenReturn("ok");

            AiRegisterSkillFunction fn = new AiRegisterSkillFunction();
            Object result = fn.compute(new Object[] {"name", "prompt", "{}"}, null);
            Assert.assertEquals("ok", result);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAiRegisterSkillFunction_noArgs() {
        AiRegisterSkillFunction fn = new AiRegisterSkillFunction();
        fn.compute(new Object[] {}, null);
    }

    // ==================== AiUpdateSkillFunction ====================

    @Test
    public void testAiUpdateSkillFunction() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);
            Mockito.when(sm.updateSkill("name", "{}")).thenReturn("ok");

            AiUpdateSkillFunction fn = new AiUpdateSkillFunction();
            Object result = fn.compute(new Object[] {"name", "{}"}, null);
            Assert.assertEquals("ok", result);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAiUpdateSkillFunction_noArgs() {
        AiUpdateSkillFunction fn = new AiUpdateSkillFunction();
        fn.compute(new Object[] {}, null);
    }

    // ==================== AiAddSkillReferenceFunction ====================

    @Test
    public void testAiAddSkillReferenceFunction() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);
            Mockito.when(sm.addReference("skill", "ref", "content")).thenReturn("ok");

            AiAddSkillReferenceFunction fn = new AiAddSkillReferenceFunction();
            Object result = fn.compute(new Object[] {"skill", "ref", "content"}, null);
            Assert.assertEquals("ok", result);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAiAddSkillReferenceFunction_noArgs() {
        AiAddSkillReferenceFunction fn = new AiAddSkillReferenceFunction();
        fn.compute(new Object[] {}, null);
    }

    // ==================== AiRemoveSkillReferenceFunction ====================

    @Test
    public void testAiRemoveSkillReferenceFunction() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);
            Mockito.when(sm.removeReference("skill", "ref")).thenReturn("ok");

            AiRemoveSkillReferenceFunction fn = new AiRemoveSkillReferenceFunction();
            Object result = fn.compute(new Object[] {"skill", "ref"}, null);
            Assert.assertEquals("ok", result);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAiRemoveSkillReferenceFunction_noArgs() {
        AiRemoveSkillReferenceFunction fn = new AiRemoveSkillReferenceFunction();
        fn.compute(new Object[] {}, null);
    }

    // ==================== AiGetSkillPromptFunction ====================

    @Test
    public void testAiGetSkillPromptFunction_foundWithRefs() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);

            SkillConfigRecord skill = new SkillConfigRecord();
            skill.name = "s1";
            skill.description = "desc";
            skill.prompt = "prompt";
            Mockito.when(sm.getSkillConfig("s1")).thenReturn(skill);

            SkillReferenceRecord ref = new SkillReferenceRecord();
            ref.refName = "r1";
            ref.content = "ref content";
            Mockito.when(sm.getReferences("s1")).thenReturn(Arrays.asList(ref));

            AiGetSkillPromptFunction fn = new AiGetSkillPromptFunction();
            Object result = fn.compute(new Object[] {"s1"}, null);
            String text = (String) result;
            Assert.assertTrue(text.contains("技能: s1"));
            Assert.assertTrue(text.contains("描述: desc"));
            Assert.assertTrue(text.contains("prompt"));
            Assert.assertTrue(text.contains("r1"));
            Assert.assertTrue(text.contains("11 字符"));
        }
    }

    @Test
    public void testAiGetSkillPromptFunction_foundNoRefs() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);

            SkillConfigRecord skill = new SkillConfigRecord();
            skill.name = "s1";
            skill.prompt = "prompt";
            Mockito.when(sm.getSkillConfig("s1")).thenReturn(skill);
            Mockito.when(sm.getReferences("s1")).thenReturn(Collections.emptyList());

            AiGetSkillPromptFunction fn = new AiGetSkillPromptFunction();
            Object result = fn.compute(new Object[] {"s1"}, null);
            String text = (String) result;
            Assert.assertTrue(text.contains("技能: s1"));
            Assert.assertFalse(text.contains("参考文档"));
        }
    }

    @Test
    public void testAiGetSkillPromptFunction_notFound() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);
            Mockito.when(sm.getSkillConfig("missing")).thenReturn(null);

            AiGetSkillPromptFunction fn = new AiGetSkillPromptFunction();
            Object result = fn.compute(new Object[] {"missing"}, null);
            Assert.assertEquals("Skill 'missing' not found or not active", result);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAiGetSkillPromptFunction_noArgs() {
        AiGetSkillPromptFunction fn = new AiGetSkillPromptFunction();
        fn.compute(new Object[] {}, null);
    }

    // ==================== AiGetReferenceFunction ====================

    @Test
    public void testAiGetReferenceFunction_found() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);

            SkillReferenceRecord ref = new SkillReferenceRecord();
            ref.refName = "r1";
            ref.content = "content1";
            Mockito.when(sm.getReferences("s1")).thenReturn(Arrays.asList(ref));

            AiGetReferenceFunction fn = new AiGetReferenceFunction();
            Object result = fn.compute(new Object[] {"s1", "r1"}, null);
            Assert.assertEquals("content1", result);
        }
    }

    @Test
    public void testAiGetReferenceFunction_notFound() {
        try (MockedStatic<SkillManager> mock = Mockito.mockStatic(SkillManager.class)) {
            SkillManager sm = Mockito.mock(SkillManager.class);
            mock.when(SkillManager::getInstance).thenReturn(sm);
            Mockito.when(sm.getReferences("s1")).thenReturn(Collections.emptyList());

            AiGetReferenceFunction fn = new AiGetReferenceFunction();
            Object result = fn.compute(new Object[] {"s1", "r1"}, null);
            Assert.assertEquals("Reference 'r1' not found in skill 's1'", result);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAiGetReferenceFunction_noArgs() {
        AiGetReferenceFunction fn = new AiGetReferenceFunction();
        fn.compute(new Object[] {}, null);
    }
}
