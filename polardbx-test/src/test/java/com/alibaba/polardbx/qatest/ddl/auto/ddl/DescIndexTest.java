package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;

@ReplicaIgnore(ignoreReason = "not support 80 expression index in replica")
public class DescIndexTest extends DDLBaseNewDBTestCase {
    String tableName = "example1";
    String TABLE_DEFINITION_FORMAT =
        "CREATE TABLE `%s` ( `id` varchar(100) NOT NULL, `name` varchar(100) NOT NULL, `a` int DEFAULT NULL, `b` text NOT NULL, first_name VARCHAR(64) NOT NULL, last_name VARCHAR(64) NOT NULL, email VARCHAR(255) NOT NULL, phone VARCHAR(32), content TEXT, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, full_name VARCHAR(130) GENERATED ALWAYS AS (CONCAT_WS(' ', first_name, last_name)) VIRTUAL, rev_phone VARCHAR(32) GENERATED ALWAYS AS (REVERSE(phone)) STORED, PRIMARY KEY (`id` desc,name), KEY `index1` (`a`,`id`(30)), KEY `index2` (`a`,`name`(30) desc), KEY `index3` (`a`, b(30) desc), KEY `index4` (`id`, b(29) desc), INDEX idx_full_name (full_name(10) asc), INDEX idx_rev_phone (rev_phone desc), INDEX rev_sub(rev_phone(10)), global index gsi_a(a) partition by key(a) partitions 10 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 partition by key(id) partitions 10";

    String tableName2 = "example2";
    String TABLE_DEFINITION_FORMAT_FULL =
        "CREATE TABLE `%s` ( `id` varchar(100) NOT NULL, `name` varchar(100) NOT NULL, `a` int DEFAULT NULL, `b` text NOT NULL, first_name VARCHAR(64) NOT NULL, last_name VARCHAR(64) NOT NULL, email VARCHAR(255) NOT NULL, phone VARCHAR(32), content TEXT, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, full_name VARCHAR(130) GENERATED ALWAYS AS (CONCAT_WS(' ', first_name, last_name)) VIRTUAL, rev_phone VARCHAR(32) GENERATED ALWAYS AS (REVERSE(phone)) STORED, PRIMARY KEY (`id` desc,name), KEY `index1` (`a`,`id`(30)), KEY `index2` (`a`,`name`(30) desc), KEY `index3` (`a`, b(30) desc), KEY `index4` (`id`, b(29) desc), INDEX idx_lower_lastname ((LOWER(last_name))), INDEX idx_full_name (full_name(10) asc), INDEX idx_rev_phone (rev_phone desc), INDEX rev_sub(rev_phone(10)), FULLTEXT INDEX ft_content (content) WITH PARSER ngram, global index gsi_a(a) partition by key(a) partitions 10 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 partition by key(id) partitions 10";
    String HINT =
        "/*+TDDL:cmd_extra(PUSHDOWN_RANGE_LIMIT=true ENABLE_AUTO_FORCE_INDEX=true ENABLE_AUTO_PAGINATION_INDEX=true ENABLE_AUTO_PAGINATION_UNION=true enable_index_selection=false SORT_EQ_PRE_COL=10)*/";

    @Before
    public void prepareTable() {
        if (!isMySQL80()) {
            return;
        }
        JdbcUtil.dropTable(getTddlConnection1(), tableName);
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(TABLE_DEFINITION_FORMAT, tableName));

        JdbcUtil.dropTable(getTddlConnection1(), tableName2);
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(TABLE_DEFINITION_FORMAT_FULL, tableName2));
    }

    @After
    public void dropTable() {
        if (!isMySQL80()) {
            return;
        }
        JdbcUtil.dropTable(getTddlConnection1(), tableName);
        JdbcUtil.dropTable(getTddlConnection1(), tableName2);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testCreate() {
        if (!isMySQL80()) {
            return;
        }
        String sql =
            HINT + String.format("explain select 1 from %s where a = 12 and b ='xx' order by id desc,name limit 10",
                tableName);
        String explain = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, getTddlConnection1()))
            .stream().flatMap(Collection::stream).map(Object::toString).collect(Collectors.joining(""));
        assertWithMessage(sql).that(explain).contains("FORCE INDEX");

        sql = HINT + String.format("explain select 1 from %s where a = 12 and b ='xx' order by id,name limit 10",
            tableName);
        explain = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, getTddlConnection1()))
            .stream().flatMap(Collection::stream).map(Object::toString).collect(Collectors.joining(""));
        assertWithMessage(sql).that(explain).doesNotContain("FORCE INDEX");

        sql = HINT + String.format("explain select 1 from %s where a = 12 order by b limit 10",
            tableName);
        explain = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, getTddlConnection1()))
            .stream().flatMap(Collection::stream).map(Object::toString).collect(Collectors.joining(""));
        assertWithMessage(sql).that(explain).doesNotContain("FORCE INDEX");
    }

}
