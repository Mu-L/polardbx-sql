package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterSecretStatement;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FastSqlToCalciteNodeVisitorExternalTest {

    private FastSqlToCalciteNodeVisitor newVisitor() {
        return new FastSqlToCalciteNodeVisitor(null, null);
    }

    private Map<String, String> map(String... kv) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return map;
    }

    @Test
    public void testVisitAlterExternalCatalogConnector() {
        MySqlAlterExternalCatalogStatement stmt = mock(MySqlAlterExternalCatalogStatement.class);
        when(stmt.getName()).thenReturn(mock(SQLName.class));
        when(stmt.getSetProperties()).thenReturn(map("connector", "mock"));
        try {
            newVisitor().visit(stmt);
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testVisitAlterExternalCatalogSecretAndProps() {
        MySqlAlterExternalCatalogStatement stmt = mock(MySqlAlterExternalCatalogStatement.class);
        when(stmt.getName()).thenReturn(mock(SQLName.class));
        when(stmt.getComment()).thenReturn("comment");
        when(stmt.getSetProperties()).thenReturn(map("secret", "s1", "k", "v"));
        try {
            newVisitor().visit(stmt);
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testVisitAlterSecret() {
        MySqlAlterSecretStatement stmt = mock(MySqlAlterSecretStatement.class);
        when(stmt.getName()).thenReturn(mock(SQLName.class));
        when(stmt.getSetProperties()).thenReturn(map("k", "v"));
        newVisitor().visit(stmt);
    }
}
