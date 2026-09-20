/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator.orc;

import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class OrcCollationTest {
    String FILE_NAME = "/tmp/collation_test.orc";
    FileSystem FILE_SYSTEM;

    private static final Configuration configuration = new Configuration();

    static {
        OrcConf.ROW_INDEX_STRIDE.setInt(configuration, 10000);
        OrcConf.COMPRESS.setString(configuration, "LZ4");
    }

    @Before
    public void before() throws IOException {
        FILE_SYSTEM = FileSystem.getLocal(new Configuration());
        Path path = new Path(FILE_NAME);
        if (FILE_SYSTEM.exists(path)) {
            FILE_SYSTEM.delete(path, false);
        }
    }

    @After
    public void after() throws IOException {
        Path path = new Path(FILE_NAME);
        if (FILE_SYSTEM.exists(path)) {
            FILE_SYSTEM.delete(path, false);
        }
    }

    @Test
    public void writeTest() throws IOException {
        TypeDescription schema =
            TypeDescription.fromString("struct<field1:varchar(65536)>");
        schema.getChildren().get(0).setAttribute(TypeDescription.CHARSET_ATTRIBUTE, "utf8mb4");
        schema.getChildren().get(0).setAttribute(TypeDescription.COLLATION_ATTRIBUTE, "utf8mb4_0900_ai_ci");

        OrcFile.WriterOptions opts = OrcFile
            .writerOptions(configuration)
            .setSchema(schema)
            .fileSystem(FILE_SYSTEM);

        VectorizedRowBatch batch = schema.createRowBatch();
        try (Writer writer = OrcFile.createWriter(new Path(FILE_NAME), opts)) {
            BytesColumnVector field3 = (BytesColumnVector) batch.cols[0];
            field3.setVal(0, "RE_ABCC".getBytes());
            field3.setVal(1, "RETBCD".getBytes());
            field3.setVal(2, "ABCDEF".getBytes());
            batch.size = 3;
            writer.addRowBatch(batch);
            batch.reset();
        }

        OrcFile.ReaderOptions readOptions = OrcFile
            .readerOptions(configuration)
            .filesystem(FILE_SYSTEM);

        try (Reader reader = OrcFile.createReader(new Path(FILE_NAME), readOptions)) {
            TypeDescription readSchema = reader.getSchema();
            Assert.assertEquals("utf8mb4",
                readSchema.getChildren().get(0).getAttributeValue(TypeDescription.CHARSET_ATTRIBUTE));
            Assert.assertEquals("utf8mb4_0900_ai_ci",
                readSchema.getChildren().get(0).getAttributeValue(TypeDescription.COLLATION_ATTRIBUTE));

            ColumnStatistics[] columnStatistics = reader.getStatistics();
            StringColumnStatistics stringColumnStatistics = (StringColumnStatistics) columnStatistics[1];
            Assert.assertEquals("ABCDEF", stringColumnStatistics.getMinimum());
            Assert.assertEquals("RETBCD", stringColumnStatistics.getMaximum());
        }

        Path path = new Path(FILE_NAME);
        if (FILE_SYSTEM.exists(path)) {
            FILE_SYSTEM.delete(path, false);
        }

        schema =
            TypeDescription.fromString("struct<field1:varchar(65536)>");
        opts = OrcFile
            .writerOptions(configuration)
            .setSchema(schema)
            .fileSystem(FILE_SYSTEM);

        batch = schema.createRowBatch();
        try (Writer writer = OrcFile.createWriter(new Path(FILE_NAME), opts)) {
            BytesColumnVector field3 = (BytesColumnVector) batch.cols[0];
            field3.setVal(0, "RE_ABCC".getBytes());
            field3.setVal(1, "RETBCD".getBytes());
            field3.setVal(2, "ABCDEF".getBytes());
            batch.size = 3;
            writer.addRowBatch(batch);
            batch.reset();
        }

        readOptions = OrcFile
            .readerOptions(configuration)
            .filesystem(FILE_SYSTEM);

        try (Reader reader = OrcFile.createReader(new Path(FILE_NAME), readOptions)) {
            TypeDescription readSchema = reader.getSchema();
            Assert.assertNull(readSchema.getChildren().get(0).getAttributeValue(TypeDescription.CHARSET_ATTRIBUTE));
            Assert.assertNull(readSchema.getChildren().get(0).getAttributeValue(TypeDescription.COLLATION_ATTRIBUTE));

            ColumnStatistics[] columnStatistics = reader.getStatistics();
            StringColumnStatistics stringColumnStatistics = (StringColumnStatistics) columnStatistics[1];
            Assert.assertEquals("ABCDEF", stringColumnStatistics.getMinimum());
            Assert.assertEquals("RE_ABCC", stringColumnStatistics.getMaximum());
        }
    }

    @Test
    public void varcharTest() throws IOException {
        try {
            VarcharType.buildVarcharType(null, "utf8mb4_0900_ai_ci");
            Assert.fail();
        } catch (Exception ignored) {
        }
        try {
            VarcharType.buildVarcharType("utf8mb4", null);
            Assert.fail();
        } catch (Exception ignored) {
        }
    }

}
