package com.alibaba.polardbx.gms.metadb.external;

/**
 * Shared constants for external catalog / external table option keys and connector types.
 * These keys are part of the protocol between CN and connector plugins.
 */
public final class ExternalCatalogConstants {

    // -------------------------------------------------------------------------
    // Option keys (used in FILES() properties, external table options, catalog props)
    // -------------------------------------------------------------------------

    public static final String OPTION_CONNECTOR = "connector";
    public static final String OPTION_SECRET = "secret";
    public static final String OPTION_FORMAT = "format";
    public static final String OPTION_PATH = "path";
    public static final String OPTION_ENGINE_TYPE = "engine_type";
    public static final String OPTION_NATIVE_QUERY_CATALOG = "native_query_catalog";
    public static final String OPTION_NATIVE_QUERY_SQL = "native_query_sql";

    // -------------------------------------------------------------------------
    // Connector type identifiers
    // -------------------------------------------------------------------------

    public static final String CONNECTOR_FILES = "files";
    public static final String CONNECTOR_MOCK = "mock";

    // -------------------------------------------------------------------------
    // DDL resource lock prefixes
    // -------------------------------------------------------------------------

    public static final String CATALOG_RESOURCE_PREFIX = "catalog:";
    public static final String SECRET_RESOURCE_PREFIX = "secret:";

    // -------------------------------------------------------------------------
    // Ephemeral table naming
    // -------------------------------------------------------------------------

    public static final String EPHEMERAL_SCHEMA_NAME = "__files_ep";
    public static final String EPHEMERAL_TABLE_PREFIX = "__files_";

    private ExternalCatalogConstants() {
    }

    public static boolean isMockConnector(String connector) {
        return CONNECTOR_MOCK.equalsIgnoreCase(connector);
    }
}
