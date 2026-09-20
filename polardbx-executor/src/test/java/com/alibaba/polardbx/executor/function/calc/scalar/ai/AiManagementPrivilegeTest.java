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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ai.ModelManager;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AiManagementPrivilegeTest {

    @Mock
    private ExecutionContext mockExecutionContext;

    @Mock
    private PrivilegeContext mockPrivilegeContext;

    @Mock
    private PolarAccountInfo mockPolarUserInfo;

    @Mock
    private ModelManager mockModelManager;

    @Before
    public void setUp() {
        Mockito.when(mockExecutionContext.getPrivilegeContext()).thenReturn(mockPrivilegeContext);
        Mockito.when(mockPrivilegeContext.getPolarUserInfo()).thenReturn(mockPolarUserInfo);
        Mockito.when(mockPrivilegeContext.getUser()).thenReturn("test_user");
        Mockito.when(mockPrivilegeContext.getHost()).thenReturn("localhost");
    }

    @Test
    public void testSuperUserCanExecuteAllManagementFunctions() {
        Mockito.when(mockExecutionContext.isSuperUser()).thenReturn(true);
        Mockito.when(mockModelManager.registerModel("model", "provider", "endpoint", "api-model", "{}"))
            .thenReturn("registered");
        Mockito.when(mockModelManager.updateModel("model", "{}"))
            .thenReturn("updated");
        Mockito.when(mockModelManager.dropModel("model"))
            .thenReturn("dropped");

        try (MockedStatic<ModelManager> mockedModelManager = Mockito.mockStatic(ModelManager.class)) {
            mockedModelManager.when(ModelManager::getInstance).thenReturn(mockModelManager);

            Assert.assertEquals("registered", new AiRegisterModelFunction().compute(
                new Object[] {"model", "provider", "endpoint", "api-model", "{}"}, mockExecutionContext));
            Assert.assertEquals("updated", new AiUpdateModelFunction().compute(
                new Object[] {"model", "{}"}, mockExecutionContext));
            Assert.assertEquals("dropped", new AiDropModelFunction().compute(
                new Object[] {"model"}, mockExecutionContext));
            Assert.assertEquals("OK", new AiUpdateFunctionFunction().compute(
                new Object[] {"AI_PROMPT", "model"}, mockExecutionContext));

            Mockito.verify(mockModelManager).registerModel("model", "provider", "endpoint", "api-model", "{}");
            Mockito.verify(mockModelManager).updateModel("model", "{}");
            Mockito.verify(mockModelManager).dropModel("model");
            Mockito.verify(mockModelManager).updateDefaultModelForFunction("AI_PROMPT", "model");
        }
    }

    @Test
    public void testNonSuperUserIsDeniedBeforeModelManagerInvocation() {
        Mockito.when(mockExecutionContext.isSuperUser()).thenReturn(false);

        try (MockedStatic<ModelManager> mockedModelManager = Mockito.mockStatic(ModelManager.class)) {
            assertDenied(new AiRegisterModelFunction(),
                new Object[] {"model", "provider", "endpoint", "api-model"}, "AI_REGISTER_MODEL");
            assertDenied(new AiUpdateModelFunction(), new Object[] {"model", "{}"}, "AI_UPDATE_MODEL");
            assertDenied(new AiDropModelFunction(), new Object[] {"model"}, "AI_DROP_MODEL");
            assertDenied(new AiUpdateFunctionFunction(),
                new Object[] {"AI_PROMPT", "model"}, "AI_UPDATE_FUNCTION");

            mockedModelManager.verifyNoInteractions();
        }
    }

    @Test
    public void testPrivilegeCheckPrecedesArgumentValidation() {
        Mockito.when(mockExecutionContext.isSuperUser()).thenReturn(false);

        assertDenied(new AiRegisterModelFunction(), new Object[] {}, "AI_REGISTER_MODEL");
        assertDenied(new AiUpdateModelFunction(), new Object[] {}, "AI_UPDATE_MODEL");
        assertDenied(new AiDropModelFunction(), new Object[] {}, "AI_DROP_MODEL");
        assertDenied(new AiUpdateFunctionFunction(), new Object[] {}, "AI_UPDATE_FUNCTION");
    }

    @Test
    public void testMissingExecutionContextIsDenied() {
        try {
            AiFunctionPrivilegeUtils.checkPrivilege(null, "AI_REGISTER_MODEL");
            Assert.fail("Missing execution context should be denied");
        } catch (TddlRuntimeException e) {
            Assert.assertEquals(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED, e.getErrorCodeType());
            Assert.assertTrue(e.getMessage().contains("AI_REGISTER_MODEL"));
            Assert.assertTrue(e.getMessage().contains("unknown"));
        }
    }

    private void assertDenied(AbstractScalarFunction target, Object[] args, String functionName) {
        try {
            target.compute(args, mockExecutionContext);
            Assert.fail(functionName + " should reject non-super users");
        } catch (TddlRuntimeException e) {
            Assert.assertEquals(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED, e.getErrorCodeType());
            Assert.assertTrue(e.getMessage().contains(functionName));
            Assert.assertTrue(e.getMessage().contains("test_user"));
            Assert.assertTrue(e.getMessage().contains("localhost"));
        }
    }
}
