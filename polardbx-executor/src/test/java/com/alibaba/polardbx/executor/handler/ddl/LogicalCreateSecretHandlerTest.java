package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateSecretJobFactory;
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
import com.alibaba.polardbx.optimizer.secret.SecretMaskUtils;
import com.alibaba.polardbx.optimizer.secret.SecretTypeRegistry;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;
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

public class LogicalCreateSecretHandlerTest {

    private LogicalSecretDdl mockDdl(String secretName, boolean ifNotExists, Map<String, String> props) {
        LogicalSecretDdl ddl = mock(LogicalSecretDdl.class);
        when(ddl.getSecretName()).thenReturn(secretName);
        when(ddl.isIfNotExists()).thenReturn(ifNotExists);
        when(ddl.getProperties()).thenReturn(props);
        return ddl;
    }

    private ExecutionContext mockEc() {
        ExecutionContext ec = mock(ExecutionContext.class);
        DdlContext ddlCtx = mock(DdlContext.class);
        when(ec.getDdlContext()).thenReturn(ddlCtx);
        when(ddlCtx.getDdlStmt()).thenReturn("CREATE SECRET s1");
        return ec;
    }

    @Test
    public void testGetObjectName() {
        LogicalSecretDdl ddl = mockDdl("s1", false, new HashMap<>());
        LogicalCreateSecretHandler handler =
            new LogicalCreateSecretHandler(mock(IRepository.class));
        assertEquals("s1", handler.getObjectName(ddl));
    }

    @Test
    public void testHandleIfNotExistsAlreadyExists() {
        LogicalSecretDdl ddl = mockDdl("s1", true, new HashMap<>());
        ExecutionContext ec = mockEc();
        Connection mockConn = mock(Connection.class);
        ExternalSecretRecord mockRecord = mock(ExternalSecretRecord.class);

        try (MockedStatic<PolarPrivilegeUtils> ppuMock = mockStatic(PolarPrivilegeUtils.class);
            MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<ExternalSecretAccessor> aMock = mockConstruction(ExternalSecretAccessor.class,
                (m, ctx) -> when(m.selectByName("s1")).thenReturn(mockRecord))) {
            mdbMock.when(MetaDbUtil::getConnection).thenReturn(mockConn);

            LogicalCreateSecretHandler handler =
                new LogicalCreateSecretHandler(mock(IRepository.class));
            Cursor cursor = handler.handle((RelNode) ddl, ec);
            assertTrue(cursor instanceof AffectRowCursor);
        }
    }

    @Test
    public void testBuildDdlJob() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "type1");
        props.put("access_key_id", "ak");
        LogicalSecretDdl ddl = mockDdl("s1", false, props);
        ExecutionContext ec = mockEc();

        SecretTypeRegistry mockStr = mock(SecretTypeRegistry.class);
        PropertyDefinition mockDef = mock(PropertyDefinition.class);

        try (MockedStatic<SecretMaskUtils> smuMock = mockStatic(SecretMaskUtils.class);
            MockedStatic<SecretTypeRegistry> strMock = mockStatic(SecretTypeRegistry.class);
            MockedConstruction<CreateSecretJobFactory> fMock = mockConstruction(CreateSecretJobFactory.class)) {
            strMock.when(SecretTypeRegistry::getInstance).thenReturn(mockStr);
            when(mockStr.get("type1")).thenReturn(mockDef);
            doNothing().when(mockDef).validateAsSecret(props);

            LogicalCreateSecretHandler handler =
                new LogicalCreateSecretHandler(mock(IRepository.class));
            DdlJob job = handler.buildDdlJob(ddl, ec);
            assertEquals(1, fMock.constructed().size());
        }
    }

    @Test
    public void testBuildDdlJobMissingType() {
        Map<String, String> props = new HashMap<>();
        LogicalSecretDdl ddl = mockDdl("s1", false, props);
        ExecutionContext ec = mockEc();

        SecretTypeRegistry mockStr = mock(SecretTypeRegistry.class);

        try (MockedStatic<SecretMaskUtils> smuMock = mockStatic(SecretMaskUtils.class);
            MockedStatic<SecretTypeRegistry> strMock = mockStatic(SecretTypeRegistry.class)) {
            strMock.when(SecretTypeRegistry::getInstance).thenReturn(mockStr);

            LogicalCreateSecretHandler handler =
                new LogicalCreateSecretHandler(mock(IRepository.class));
            handler.buildDdlJob(ddl, ec);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("'type' is required"));
        }
    }

    @Test
    public void testBuildDdlJobUnknownType() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "unknown");
        LogicalSecretDdl ddl = mockDdl("s1", false, props);
        ExecutionContext ec = mockEc();

        SecretTypeRegistry mockStr = mock(SecretTypeRegistry.class);

        try (MockedStatic<SecretMaskUtils> smuMock = mockStatic(SecretMaskUtils.class);
            MockedStatic<SecretTypeRegistry> strMock = mockStatic(SecretTypeRegistry.class)) {
            strMock.when(SecretTypeRegistry::getInstance).thenReturn(mockStr);
            when(mockStr.get("unknown")).thenReturn(null);

            LogicalCreateSecretHandler handler =
                new LogicalCreateSecretHandler(mock(IRepository.class));
            handler.buildDdlJob(ddl, ec);
            fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Unknown secret type"));
        }
    }
}
