package com.alibaba.polardbx.druid.sql;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterSecretStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateSecretStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDescribeExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDropSecretStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRefreshExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowConnectorsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateSecretStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowExternalCatalogsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowSecretsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlParameterizedVisitor;
import org.junit.Test;

public class MySqlParameterizedVisitorExternalTest {

    @Test
    public void testExternalCatalogVisitEndVisit() {
        MySqlParameterizedVisitor visitor = new MySqlParameterizedVisitor();

        SQLStatement[] stmts = new SQLStatement[] {
            new MySqlAlterExternalCatalogStatement(),
            new MySqlRefreshExternalCatalogStatement(),
            new MySqlCreateSecretStatement(),
            new MySqlDropSecretStatement(),
            new MySqlAlterSecretStatement(),
            new MySqlShowExternalCatalogsStatement(),
            new MySqlShowSecretsStatement(),
            new MySqlShowCreateExternalCatalogStatement(),
            new MySqlDescribeExternalCatalogStatement(),
            new MySqlShowCreateSecretStatement(),
            new MySqlShowConnectorsStatement()
        };

        for (SQLStatement stmt : stmts) {
            stmt.accept(visitor);
        }
    }

    @Test
    public void testDirectVisitEndVisit() {
        MySqlParameterizedVisitor visitor = new MySqlParameterizedVisitor();

        MySqlAlterExternalCatalogStatement alterCatalog = new MySqlAlterExternalCatalogStatement();
        visitor.visit(alterCatalog);
        visitor.endVisit(alterCatalog);

        MySqlRefreshExternalCatalogStatement refreshCatalog = new MySqlRefreshExternalCatalogStatement();
        visitor.visit(refreshCatalog);
        visitor.endVisit(refreshCatalog);

        MySqlCreateSecretStatement createSecret = new MySqlCreateSecretStatement();
        visitor.visit(createSecret);
        visitor.endVisit(createSecret);

        MySqlDropSecretStatement dropSecret = new MySqlDropSecretStatement();
        visitor.visit(dropSecret);
        visitor.endVisit(dropSecret);

        MySqlAlterSecretStatement alterSecret = new MySqlAlterSecretStatement();
        visitor.visit(alterSecret);
        visitor.endVisit(alterSecret);

        MySqlShowExternalCatalogsStatement showCatalogs = new MySqlShowExternalCatalogsStatement();
        visitor.visit(showCatalogs);
        visitor.endVisit(showCatalogs);

        MySqlShowSecretsStatement showSecrets = new MySqlShowSecretsStatement();
        visitor.visit(showSecrets);
        visitor.endVisit(showSecrets);

        MySqlShowCreateExternalCatalogStatement showCreateCatalog = new MySqlShowCreateExternalCatalogStatement();
        visitor.visit(showCreateCatalog);
        visitor.endVisit(showCreateCatalog);

        MySqlDescribeExternalCatalogStatement describeCatalog = new MySqlDescribeExternalCatalogStatement();
        visitor.visit(describeCatalog);
        visitor.endVisit(describeCatalog);

        MySqlShowCreateSecretStatement showCreateSecret = new MySqlShowCreateSecretStatement();
        visitor.visit(showCreateSecret);
        visitor.endVisit(showCreateSecret);

        MySqlShowConnectorsStatement showConnectors = new MySqlShowConnectorsStatement();
        visitor.visit(showConnectors);
        visitor.endVisit(showConnectors);
    }
}
