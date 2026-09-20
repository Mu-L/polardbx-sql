package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end reproduction for the NullPointerException raised by PartitionGather while gathering
 * partitions for an INSERT ... ON DUPLICATE KEY UPDATE statement during a DN HA switchover.
 *
 * <p>The switchover-check path ({@code TConnection.rescheduleIfSwitchover}) can only be reached on
 * the server when a real DN leader change is in progress, which a JDBC client cannot trigger. To
 * reproduce the issue purely through SQL, this test relies on the test-only connection parameter
 * {@code FORCE_SWITCHOVER_CHECK_FOR_TEST}:
 * <ul>
 *     <li>It forces the switchover-check path (PartitionGather over the plan) to run regardless of
 *     the real leader-changing state.</li>
 *     <li>It makes PartitionGather rethrow any gather error instead of swallowing it, so the NPE
 *     surfaces as a SQL failure.</li>
 * </ul>
 *
 * <p>The ON DUPLICATE KEY UPDATE list references an existing column
 * ({@code sub_balance_raw = sub_balance_raw + values(sub_balance_raw)}); this introduces a column
 * reference that PartitionGather evaluates against a null row, which is exactly what triggers the
 * NPE during switchover.
 *
 * <ul>
 *     <li>Buggy code: the hinted upsert fails with a NullPointerException -&gt; test RED.</li>
 *     <li>Fixed code: the hinted upsert executes successfully -&gt; test GREEN.</li>
 * </ul>
 */
public class UpsertSwitchoverGatherTest extends AutoCrudBasedLockTestCase {

    private static final String TABLE_NAME = "upsert_switchover_gather_tb";

    private static final String FORCE_SWITCHOVER_HINT =
        "/*+TDDL:cmd_extra(FORCE_SWITCHOVER_CHECK_FOR_TEST=true)*/ ";

    @Before
    public void prepareTable() {
        JdbcUtil.executeUpdate(tddlConnection, "drop table if exists " + TABLE_NAME);
        // A GSI covering an updated column (sub_balance_raw) makes the upsert go through the
        // LOGICAL execution strategy (updateGsi -> cannot push down), which is exactly the case
        // that reproduces the switchover gather NPE - the same shape as the production table.
        final String createTable = "create table " + TABLE_NAME + " ("
            + "  address varchar(64) not null,"
            + "  chain_index varchar(16) not null,"
            + "  token_address varchar(128) not null,"
            + "  coin_type int,"
            + "  sub_balance_raw bigint,"
            + "  sub_balance_height bigint,"
            + "  ext_data varchar(256),"
            + "  sub_last_transaction_time bigint,"
            + "  sub_upd_time bigint,"
            + "  primary key(address, chain_index, token_address),"
            + "  global index g_sub_balance_raw(sub_balance_raw) covering(address)"
            + "    partition by key(sub_balance_raw) partitions 8"
            + ") partition by key(address) partitions 8";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // Seed rows so the upsert hits ON DUPLICATE KEY UPDATE on the second run.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into " + TABLE_NAME
            + " (address, chain_index, token_address, coin_type, sub_balance_raw, sub_balance_height,"
            + "  ext_data, sub_last_transaction_time, sub_upd_time) values"
            + " ('0xe75e','8453','t',0,1,47135497,'',1781060341,1781060341969),"
            + " ('0xd057','8453','t',0,1,47135497,'',1781060341,1781060341969),"
            + " ('0x278d','8453','t',0,1,47135497,'',1781060341,1781060341969)");
    }

    @After
    public void cleanup() {
        JdbcUtil.executeUpdate(tddlConnection, "drop table if exists " + TABLE_NAME);
    }

    /**
     * Multi-row upsert executed through the forced switchover-check path.
     */
    @Test
    public void testMultiRowUpsertDuringSwitchover() {
        final String upsert = FORCE_SWITCHOVER_HINT + "insert into " + TABLE_NAME
            + " (address, chain_index, token_address, coin_type, sub_balance_raw, sub_balance_height,"
            + "  ext_data, sub_last_transaction_time, sub_upd_time) values"
            + " ('0xe75e','8453','t',0,2,47135498,'',1781060342,1781060342000),"
            + " ('0xd057','8453','t',0,3,47135498,'',1781060342,1781060342000),"
            + " ('0x278d','8453','t',0,4,47135498,'',1781060342,1781060342000)"
            + " on duplicate key update"
            + "  coin_type = values(coin_type),"
            // Self-reference forces a column reference that PartitionGather evaluates with a null
            // row while gathering partitions during switchover, which triggers the NPE.
            + "  sub_balance_raw = sub_balance_raw + values(sub_balance_raw),"
            + "  sub_balance_height = values(sub_balance_height),"
            + "  ext_data = values(ext_data),"
            + "  sub_last_transaction_time = values(sub_last_transaction_time),"
            + "  sub_upd_time = values(sub_upd_time)";

        // On buggy code this throws (NPE surfaced via the test hook); on fixed code it succeeds.
        JdbcUtil.executeUpdateSuccess(tddlConnection, upsert);

        final String count = JdbcUtil.executeQueryAndGetFirstStringResult(
            "select count(*) from " + TABLE_NAME, tddlConnection);
        Assert.assertEquals("3", count);
    }
}
