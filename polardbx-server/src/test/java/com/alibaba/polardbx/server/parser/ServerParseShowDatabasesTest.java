package com.alibaba.polardbx.server.parser;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class ServerParseShowDatabasesTest {

    private int parse(String sql) {
        return ServerParseShow.parse(ByteString.from(sql), "SHOW".length());
    }

    @Test
    public void plainShowDatabasesIsHandledLocally() {
        Assert.assertEquals(ServerParseShow.DATABASES, parse("SHOW DATABASES"));
        Assert.assertEquals(ServerParseShow.DATABASES, parse("SHOW DATABASES;"));
        Assert.assertEquals(ServerParseShow.DATABASES, parse("SHOW DATABASES "));
        Assert.assertEquals(ServerParseShow.DATABASES, parse("show databases"));
    }

    @Test
    public void showDatabasesFromCatalogFallsThroughToFullParser() {
        Assert.assertEquals(ServerParseShow.OTHER, parse("SHOW DATABASES FROM mycat"));
        Assert.assertEquals(ServerParseShow.OTHER, parse("SHOW DATABASES  FROM mycat"));
        Assert.assertEquals(ServerParseShow.OTHER, parse("show databases from mycat"));
    }

    @Test
    public void showDatabasesFromCatalogWithLineBreakFallsThroughToFullParser() {
        Assert.assertEquals(ServerParseShow.OTHER, parse("SHOW DATABASES\nFROM mycat"));
        Assert.assertEquals(ServerParseShow.OTHER, parse("SHOW DATABASES\r\nFROM mycat"));
        Assert.assertEquals(ServerParseShow.OTHER, parse("SHOW DATABASES\tFROM mycat"));
        Assert.assertEquals(ServerParseShow.OTHER, parse("SHOW DATABASES \n\t FROM mycat"));
    }

    @Test
    public void semicolonTerminatedStatementIsNotTreatedAsFromClause() {
        Assert.assertEquals(ServerParseShow.DATABASES, parse("SHOW DATABASES;\nFROM"));
    }

    @Test
    public void showDatabasesLikeIsHandledLocally() {
        Assert.assertEquals(ServerParseShow.DATABASES, parse("SHOW DATABASES LIKE 'x%'"));
        Assert.assertEquals(ServerParseShow.DATABASES, parse("SHOW DATABASES\nLIKE 'x%'"));
    }
}
