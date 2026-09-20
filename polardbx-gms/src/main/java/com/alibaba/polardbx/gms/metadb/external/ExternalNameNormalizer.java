package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;

import java.util.Locale;

/**
 * Single entry point for reading external catalog / secret names out of a druid AST.
 * The lexer keeps quote characters inside stringVal, so a name read straight from the
 * AST is `cat` rather than cat; every reader must come through here or the same object
 * ends up under two different names.
 */
public final class ExternalNameNormalizer {

    public static final String WILDCARD = "*";

    private ExternalNameNormalizer() {
    }

    public static String normalize(String rawName) {
        // Pure delegation on purpose: the value of this method is being the one named
        // entry point, so that a call site reading a name is easy to grep for.
        // normalizeNoTrim already passes null and "*" through unchanged.
        return SQLUtils.normalizeNoTrim(rawName);
    }

    public static String normalize(SQLName name) {
        return name == null ? null : normalize(name.getSimpleName());
    }

    public static String normalizeToLower(SQLName name) {
        String normalized = normalize(name);
        return normalized == null ? null : normalized.toLowerCase(Locale.ROOT);
    }

    /**
     * Structural parse only: no existence check and no case folding, so both the
     * privilege path (which needs wildcards) and the query path can share it.
     */
    public static ThreePartName parseThreePartName(SQLExpr expr) {
        if (!(expr instanceof SQLPropertyExpr)) {
            return null;
        }
        SQLPropertyExpr tableExpr = (SQLPropertyExpr) expr;
        if (!(tableExpr.getOwner() instanceof SQLPropertyExpr)) {
            return null;
        }
        SQLPropertyExpr catalogDbExpr = (SQLPropertyExpr) tableExpr.getOwner();
        String catalogName = catalogDbExpr.getOwner() instanceof SQLAllColumnExpr
            ? WILDCARD
            : normalize(catalogDbExpr.getOwnerName());
        String dbName = normalize(catalogDbExpr.getName());
        String tableName = normalize(tableExpr.getName());
        if (catalogName == null || dbName == null || tableName == null) {
            return null;
        }
        return new ThreePartName(catalogName, dbName, tableName);
    }

    public static ThreePartName resolveExternalTable(SQLExpr expr) {
        ThreePartName parsed = parseThreePartName(expr);
        if (parsed == null || parsed.isCatalogWildcard()) {
            return null;
        }
        if (!ExternalCatalogManager.getInstance().exists(parsed.getCatalogName())) {
            return null;
        }
        return parsed;
    }

    public static final class ThreePartName {

        private final String catalogName;
        private final String dbName;
        private final String tableName;

        ThreePartName(String catalogName, String dbName, String tableName) {
            this.catalogName = catalogName;
            this.dbName = dbName;
            this.tableName = tableName;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTableName() {
            return tableName;
        }

        public boolean isCatalogWildcard() {
            return WILDCARD.equals(catalogName);
        }
    }
}
