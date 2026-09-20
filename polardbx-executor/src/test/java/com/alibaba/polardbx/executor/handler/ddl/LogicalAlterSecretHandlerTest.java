package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterSecretJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretAccessor;
import com.alibaba.polardbx.gms.metadb.external.ExternalSecretRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalSecretDdl;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import com.alibaba.polardbx.optimizer.secret.SecretMaskUtils;
import com.alibaba.polardbx.optimizer.secret.SecretTypeRegistry;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalAlterSecretHandlerTest {

    private LogicalSecretDdl mockDdl(String secretName, Map<String, String> props) {
        LogicalSecretDdl ddl = mock(LogicalSecretDdl.class);
        when(ddl.getSecretName()).thenReturn(secretName);
        when(ddl.getProperties()).thenReturn(props);
        return ddl;
    }

    private ExecutionContext mockEc() {
        ExecutionContext ec = mock(ExecutionContext.class);
        DdlContext ddlCtx = mock(DdlContext.class);
        when(ec.getDdlContext()).thenReturn(ddlCtx);
        when(ddlCtx.getDdlStmt()).thenReturn("ALTER SECRET s1");
        return ec;
    }

    @Test
    public void testGetObjectName() {
        LogicalSecretDdl ddl = mockDdl("s1", new HashMap<>());
        LogicalAlterSecretHandler handler =
            new LogicalAlterSecretHandler(mock(IRepository.class));
        assertEquals("s1", handler.getObjectName(ddl));
    }

    @Test
    public void testBuildDdlJobFromCache() {
        Map<String, String> props = new HashMap<>();
        props.put("access_key_id", "ak");
        LogicalSecretDdl ddl = mockDdl("s1", props);
        ExecutionContext ec = mockEc();

        SecretManager mockSm = mock(SecretManager.class);
        SecretManager.SecretInfo info =
            new SecretManager.SecretInfo("s1", "type1", props);
        SecretTypeRegistry mockStr = mock(SecretTypeRegistry.class);
        PropertyDefinition mockDef = mock(PropertyDefinition.class);

        try (MockedStatic<SecretMaskUtils> smuMock = mockStatic(SecretMaskUtils.class);
            MockedStatic<PolarPrivilegeUtils> ppuMock = mockStatic(PolarPrivilegeUtils.class);
            MockedStatic<SecretManager> smMock = mockStatic(SecretManager.class);
            MockedStatic<SecretTypeRegistry> strMock = mockStatic(SecretTypeRegistry.class);
            MockedConstruction<AlterSecretJobFactory> fMock = mockConstruction(AlterSecretJobFactory.class)) {
            smuMock.when(() -> SecretMaskUtils.mask("ALTER SECRET s1")).thenReturn("masked");
            smMock.when(SecretManager::getInstance).thenReturn(mockSm);
            when(mockSm.getInfo("s1")).thenReturn(info);
            strMock.when(SecretTypeRegistry::getInstance).thenReturn(mockStr);
            when(mockStr.get("type1")).thenReturn(mockDef);
            doNothing().when(mockDef).validateAsSecret(props);

            LogicalAlterSecretHandler handler =
                new LogicalAlterSecretHandler(mock(IRepository.class));
            DdlJob job = handler.buildDdlJob(ddl, ec);
            assertEquals(1, fMock.constructed().size());
        }
    }

    @Test
    public void testBuildDdlJobFallbackToGms() {
        Map<String, String> props = new HashMap<>();
        LogicalSecretDdl ddl = mockDdl("s1", props);
        ExecutionContext ec = mockEc();

        SecretManager mockSm = mock(SecretManager.class);
        SecretTypeRegistry mockStr = mock(SecretTypeRegistry.class);
        PropertyDefinition mockDef = mock(PropertyDefinition.class);
        Connection mockConn = mock(Connection.class);
        ExternalSecretRecord mockRecord = mock(ExternalSecretRecord.class);

        try (MockedStatic<SecretMaskUtils> smuMock = mockStatic(SecretMaskUtils.class);
            MockedStatic<PolarPrivilegeUtils> ppuMock = mockStatic(PolarPrivilegeUtils.class);
            MockedStatic<SecretManager> smMock = mockStatic(SecretManager.class);
            MockedStatic<SecretTypeRegistry> strMock = mockStatic(SecretTypeRegistry.class);
            MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalSecretAccessor> aMock = mockConstruction(ExternalSecretAccessor.class,
                (m, ctx) -> when(m.selectByName("s1")).thenReturn(mockRecord));
            MockedConstruction<AlterSecretJobFactory> fMock = mockConstruction(AlterSecretJobFactory.class)) {
            smMock.when(SecretManager::getInstance).thenReturn(mockSm);
            when(mockSm.getInfo("s1")).thenReturn(null);
            when(mockRecord.getType()).thenReturn("type1");
            mdbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);
            strMock.when(SecretTypeRegistry::getInstance).thenReturn(mockStr);
            when(mockStr.get("type1")).thenReturn(mockDef);
            doNothing().when(mockDef).validateAsSecret(props);

            LogicalAlterSecretHandler handler =
                new LogicalAlterSecretHandler(mock(IRepository.class));
            handler.buildDdlJob(ddl, ec);
        }
    }

    @Test
    public void testBuildDdlJobTypeMismatch() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "type2");
        LogicalSecretDdl ddl = mockDdl("s1", props);
        ExecutionContext ec = mockEc();

        SecretManager mockSm = mock(SecretManager.class);
        SecretManager.SecretInfo info =
            new SecretManager.SecretInfo("s1", "type1", props);

        try (MockedStatic<SecretMaskUtils> smuMock = mockStatic(SecretMaskUtils.class);
            MockedStatic<PolarPrivilegeUtils> ppuMock = mockStatic(PolarPrivilegeUtils.class);
            MockedStatic<SecretManager> smMock = mockStatic(SecretManager.class)) {
            smMock.when(SecretManager::getInstance).thenReturn(mockSm);
            when(mockSm.getInfo("s1")).thenReturn(info);

            LogicalAlterSecretHandler handler =
                new LogicalAlterSecretHandler(mock(IRepository.class));
            handler.buildDdlJob(ddl, ec);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Cannot change secret type"));
        }
    }

    @Test
    public void testBuildDdlJobUnknownType() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "unknown_type");
        LogicalSecretDdl ddl = mockDdl("s1", props);
        ExecutionContext ec = mockEc();

        SecretManager mockSm = mock(SecretManager.class);
        SecretTypeRegistry mockStr = mock(SecretTypeRegistry.class);
        Connection mockConn = mock(Connection.class);

        try (MockedStatic<SecretMaskUtils> smuMock = mockStatic(SecretMaskUtils.class);
            MockedStatic<PolarPrivilegeUtils> ppuMock = mockStatic(PolarPrivilegeUtils.class);
            MockedStatic<SecretManager> smMock = mockStatic(SecretManager.class);
            MockedStatic<SecretTypeRegistry> strMock = mockStatic(SecretTypeRegistry.class);
            MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalSecretAccessor> aMock = mockConstruction(ExternalSecretAccessor.class,
                (m, ctx) -> when(m.selectByName("s1")).thenReturn(null))) {
            smMock.when(SecretManager::getInstance).thenReturn(mockSm);
            when(mockSm.getInfo("s1")).thenReturn(null);
            mdbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);
            strMock.when(SecretTypeRegistry::getInstance).thenReturn(mockStr);
            when(mockStr.get("unknown_type")).thenReturn(null);

            LogicalAlterSecretHandler handler =
                new LogicalAlterSecretHandler(mock(IRepository.class));
            handler.buildDdlJob(ddl, ec);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Unknown secret type"));
        }
    }
}
