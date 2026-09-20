package com.alibaba.polardbx.qatest.cdc.random;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TableMeta implements Serializable {

    @Getter
    @Setter
    private String schema;
    @Getter
    @Setter
    private String table;
    @Getter
    private List<FieldMeta> fields = new ArrayList<>();
    @Getter
    @Setter
    private String ddl;
    @Getter
    private String charset;
    @Setter
    private boolean useImplicitPk = false;
    @Getter
    @Setter
    private Map<String, IndexMeta> indexes = new HashMap<>();

    public TableMeta() {

    }

    public TableMeta(String schema, String table, List<FieldMeta> fields) {
        this.schema = schema;
        this.table = table;
        this.fields = fields;
        initParent();
    }

    private void initParent() {
        for (FieldMeta fm : fields) {
            fm.setParent(this);
        }
    }

    public String getFullName() {
        return schema + "." + table;
    }

    public void setFields(List<FieldMeta> fields) {
        this.fields = fields;
        initParent();
    }

    public FieldMeta getFieldMetaByName(String name) {
        return getFieldMetaByName(name, false);
    }

    public FieldMeta getFieldMetaByName(String name, boolean returnNullIfNotExist) {
        for (FieldMeta meta : fields) {
            if (meta.getColumnName().equalsIgnoreCase(name)) {
                return meta;
            }
        }

        if (returnNullIfNotExist) {
            return null;
        } else {
            throw new RuntimeException("unknown column : " + name + " for table :" + table);
        }
    }

    public List<FieldMeta> getPrimaryFields() {
        List<FieldMeta> primaries = new ArrayList<FieldMeta>();
        for (FieldMeta meta : fields) {
            if (meta.isKey()) {
                primaries.add(meta);
            }
        }

        return primaries;
    }

    public void addFieldMeta(FieldMeta fieldMeta) {
        fieldMeta.setParent(this);
        this.fields.add(fieldMeta);
    }

    public void setCharset(String charset) {
        this.charset = SQLUtils.normalize(charset);
        preProcessCharset();
    }

    public boolean getUseImplicitPk() {
        return this.useImplicitPk;
    }

    //https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-utf8mb3.html
    private void preProcessCharset() {
        if ("utf8mb3".equalsIgnoreCase(charset)) {
            charset = "utf8";
        }
    }

    @Override
    public String toString() {
        StringBuilder data = new StringBuilder();
        data.append("TableMeta [schema=" + schema + ", table=" + table + ", charset=" + charset + ", fileds=");
        for (FieldMeta field : fields) {
            data.append("\n\t").append(field.toString());
        }
        for (IndexMeta indexMeta : indexes.values()) {
            data.append("\n\t").append(indexMeta.toString());
        }
        data.append("\n]");
        return data.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableMeta tableMeta = (TableMeta) o;
        return useImplicitPk == tableMeta.useImplicitPk && Objects.equals(schema, tableMeta.schema)
            && Objects.equals(table, tableMeta.table) && Objects.equals(fields, tableMeta.fields)
            && Objects.equals(ddl, tableMeta.ddl) && Objects.equals(charset, tableMeta.charset)
            && Objects.equals(indexes, tableMeta.indexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, table, fields, ddl, charset, useImplicitPk, indexes);
    }

    public static class FieldMeta implements Serializable {

        @Getter
        private String columnName;
        @Getter
        private String columnType;
        @Getter
        @Setter
        private boolean nullable;
        @Getter
        @Setter
        private boolean key;
        @Getter
        @Setter
        private String defaultValue;
        @Getter
        @Setter
        private boolean unique;
        private String charset;
        @Getter
        private boolean binary;
        @Getter
        private boolean unsigned;
        @Setter
        @Getter
        private boolean generated;
        @Getter
        @Setter
        private boolean implicitPk;
        @Getter
        @Setter
        private boolean onUpdate;
        @Setter
        private TableMeta parent;

        public FieldMeta() {

        }

        public FieldMeta(String columnName, String columnType, boolean nullable, boolean key, String defaultValue,
                         boolean unique) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.nullable = nullable;
            this.key = key;
            this.defaultValue = defaultValue;
            this.unique = unique;
            preProcessColumnName();
            preProcessColumnType();
        }

        public FieldMeta(String columnName, String columnType, boolean nullable, boolean key, String defaultValue,
                         boolean unique, String charset) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.nullable = nullable;
            this.key = key;
            this.defaultValue = defaultValue;
            this.unique = unique;
            this.charset = SQLUtils.normalize(charset);
            preProcessColumnName();
            preProcessCharset();
            preProcessColumnType();
        }

        private void preProcessColumnType() {
            binary = StringUtils.containsIgnoreCase(columnType, "VARBINARY")
                || StringUtils.containsIgnoreCase(columnType, "BINARY");
            unsigned = StringUtils.containsIgnoreCase(columnType, "unsigned");
        }

        private void preProcessCharset() {
            if ("utf8mb3".equalsIgnoreCase(charset)) {
                charset = "utf8";
            }
        }

        private void preProcessColumnName() {
            if ("__#alibaba_rds_row_id#__".equalsIgnoreCase(columnName)) {
                implicitPk = true;
            }
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
            preProcessColumnName();
        }

        public void setColumnType(String columnType) {
            this.columnType = columnType;
            preProcessColumnType();
        }

        public String getCharset() {
            if (StringUtils.isNotBlank(charset)) {
                return charset;
            }
            if (parent != null) {
                return parent.getCharset();
            }
            return null;
        }

        public void setCharset(String charset) {
            this.charset = SQLUtils.normalize(charset);
            preProcessCharset();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FieldMeta fieldMeta = (FieldMeta) o;
            return nullable == fieldMeta.nullable &&
                key == fieldMeta.key &&
                unique == fieldMeta.unique &&
                binary == fieldMeta.binary &&
                unsigned == fieldMeta.unsigned &&
                generated == fieldMeta.generated &&
                implicitPk == fieldMeta.implicitPk &&
                Objects.equals(columnName, fieldMeta.columnName) &&
                Objects.equals(columnType, fieldMeta.columnType) &&
                Objects.equals(defaultValue, fieldMeta.defaultValue) &&
                Objects.equals(charset, fieldMeta.charset);
        }

        @Override
        public int hashCode() {
            return Objects
                .hash(columnName, columnType, nullable, key, defaultValue, unique, charset, binary, unsigned, generated,
                    implicitPk);
        }

        @Override
        public String toString() {
            return "FieldMeta ["
                + "columnName=" + columnName
                + ", columnType=" + columnType
                + ", charset=" + charset
                + ", nullable=" + nullable
                + ", key=" + key
                + ", defaultValue=" + defaultValue
                + ", unique=" + unique
                + ", binary=" + binary
                + ", unsigned=" + unsigned
                + ", generated=" + generated
                + ", implicitPk=" + implicitPk
                + "]";
        }

    }

    @Data
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IndexMeta implements Serializable {
        private String indexName;
        private String indexType;
        private boolean columnar;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IndexMeta indexMeta = (IndexMeta) o;
            return columnar == indexMeta.columnar && Objects.equals(indexName, indexMeta.indexName)
                && Objects.equals(indexType, indexMeta.indexType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, indexType, columnar);
        }
    }
}
