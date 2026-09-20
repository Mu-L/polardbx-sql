package com.alibaba.polardbx.optimizer.external.files;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EphemeralFilesSchemaManagerTest {

    private TableMeta createTestMeta(String tableName) {
        List<ColumnMeta> columns = new ArrayList<>();
        Field f = new Field(DataTypes.StringType);
        f.setOriginColumnName("col1");
        f.setOriginTableName(tableName);
        columns.add(new ColumnMeta(tableName, "col1", null, f));
        return new TableMeta(EphemeralFilesSchemaManager.SCHEMA_NAME, tableName,
            columns, null, Collections.emptyList(), false, TableStatus.PUBLIC, 0, 0);
    }

    @Test
    public void testPutAndGetTable() {
        EphemeralFilesSchemaManager sm = new EphemeralFilesSchemaManager();
        TableMeta meta = createTestMeta("t1");
        sm.putTable("t1", meta);

        TableMeta result = sm.getTable("t1");
        Assert.assertSame(meta, result);
    }

    @Test(expected = TableNotFoundException.class)
    public void testGetNonExistentTableThrows() {
        EphemeralFilesSchemaManager sm = new EphemeralFilesSchemaManager();
        sm.getTable("no_such_table");
    }

    @Test
    public void testSchemaName() {
        EphemeralFilesSchemaManager sm = new EphemeralFilesSchemaManager();
        Assert.assertEquals("__files_ep", sm.getSchemaName());
    }

    @Test
    public void testGetAllTables() {
        EphemeralFilesSchemaManager sm = new EphemeralFilesSchemaManager();
        sm.putTable("t1", createTestMeta("t1"));
        sm.putTable("t2", createTestMeta("t2"));

        Assert.assertEquals(2, sm.getAllTables().size());
    }
}
