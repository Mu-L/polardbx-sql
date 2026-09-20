package org.apache.calcite.sql.pretty;

import org.apache.calcite.sql.SqlDialect;

/**
 * Pretty printer for create view SQL statements
 *
 * @author jilong.ljl
 */
public class SqlViewPrettyWriter extends SqlPrettyWriter {

    public SqlViewPrettyWriter(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    public boolean forView() {
        return true;
    }
}
