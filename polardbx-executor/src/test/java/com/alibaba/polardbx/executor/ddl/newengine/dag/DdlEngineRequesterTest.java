package com.alibaba.polardbx.executor.ddl.newengine.dag;

import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class DdlEngineRequesterTest {

    @BeforeClass
    public static void beforeClass() {
        // Prevent MetaDbInstConfigManager from connecting to MetaDB in test environment.
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        // Force initialization with empty Properties before any background thread uses it.
        MetaDbInstConfigManager manager = MetaDbInstConfigManager.getInstance();
        // Set a very large scheduler delay so the DdlPlanScanner returns immediately
        // at the delay check before reaching ExecUtils.hasLeadership() which NPEs in test env.
        manager.getCnVariableConfigMap().setProperty("DDL_PLAN_SCHEDULER_DELAY", "99999999999");
    }

    @AfterClass
    public static void afterClass() {
        MetaDbInstConfigManager.setConfigFromMetaDb(true);
    }

    // ===== Helper methods =====

    /**
     * Build expected nodeInfo for an EmptyTask in READY state.
     * EmptyTask extends BaseValidateTask which calls onExceptionTryRollback(),
     * so exceptionAction is ROLLBACK. State defaults to READY. No cost or remark.
     * Format from AbstractDdlTask.nodeInfo():
     * "%s [shape=record  %s label=\"{%s|taskId:%s|onException:%s|state:%s%s%s%s}\"];"
     * with empty color (READY state) => 3 spaces between "record" and "label".
     */
    private static String expectedNode(long taskId) {
        return String.format(
            "%s [shape=record   label=\"{EmptyTask|taskId:%s|onException:ROLLBACK|state:READY}\"];",
            taskId, taskId);
    }

    private static String expectedEdge(long source, long target) {
        return source + " -> " + target;
    }

    /**
     * Extract content lines from a DAG string, excluding structure markers.
     */
    private static Set<String> dagContentLines(String dag) {
        return Arrays.stream(dag.split("\n"))
            .map(String::trim)
            .filter(s -> !s.isEmpty() && !s.equals("digraph G {") && !s.equals("}"))
            .collect(Collectors.toSet());
    }

    private DdlEngineRequester buildRequester(ExecutableDdlJob ddlJob, DdlContext ddlContext) {
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        Mockito.when(ec.getDdlContext()).thenReturn(ddlContext);
        return new DdlEngineRequester(ddlJob, ec, ddlContext);
    }

    // ===== ID generation tests =====

    @Test
    public void testExecute() {
        DdlJob ddlJob = Mockito.mock(DdlJob.class);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        DdlContext ddlContext = Mockito.mock(DdlContext.class);
        Mockito.when(ddlContext.getSchemaName()).thenReturn("information_schema");
        DdlEngineRequester requester = new DdlEngineRequester(ddlJob, ec, ddlContext);
        try {
            requester.execute();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("The DDL job can not be executed"));
        }
    }

    @Test
    public void testGenerateIds_jobIdAlways1() {
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        EmptyTask task = new EmptyTask("s");
        ddlJob.addTask(task);

        DdlContext ddlContext = new DdlContext();
        DdlEngineRequester requester = buildRequester(ddlJob, ddlContext);
        requester.generateIds();

        Assert.assertEquals(1L, ddlContext.getJobId());
        Assert.assertEquals(Long.valueOf(2L), task.getTaskId());
        Assert.assertEquals(Long.valueOf(1L), task.getJobId());
    }

    @Test
    public void testGenerateIds_sequentialIds() {
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        EmptyTask t1 = new EmptyTask("s");
        EmptyTask t2 = new EmptyTask("s");
        EmptyTask t3 = new EmptyTask("s");
        ddlJob.addSequentialTasks(Arrays.asList(t1, t2, t3));

        DdlContext ddlContext = new DdlContext();
        DdlEngineRequester requester = buildRequester(ddlJob, ddlContext);
        requester.generateIds();

        Assert.assertEquals(1L, ddlContext.getJobId());
        // Task IDs are 2,3,4 (order depends on ConcurrentHashMap iteration)
        Set<Long> ids = new HashSet<>(Arrays.asList(t1.getTaskId(), t2.getTaskId(), t3.getTaskId()));
        Assert.assertEquals(new HashSet<>(Arrays.asList(2L, 3L, 4L)), ids);
    }

    @Test
    public void testGenerateIds_emptyJob() {
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        DdlContext ddlContext = new DdlContext();
        DdlEngineRequester requester = buildRequester(ddlJob, ddlContext);
        requester.generateIds();

        Assert.assertEquals(1L, ddlContext.getJobId());
    }

    // ===== DAG precision validation tests =====

    @Test
    public void testDag_singleTask() {
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        EmptyTask task = new EmptyTask("s");
        ddlJob.addTask(task);

        DdlContext ddlContext = new DdlContext();
        DdlEngineRequester requester = buildRequester(ddlJob, ddlContext);
        requester.generateIds();

        String dag = ddlJob.visualizeTasks();
        Assert.assertTrue(dag.startsWith("digraph G {"));
        Assert.assertTrue(dag.trim().endsWith("}"));

        long id = task.getTaskId();
        Set<String> expected = new HashSet<>(Arrays.asList(expectedNode(id)));
        Assert.assertEquals(expected, dagContentLines(dag));
    }

    @Test
    public void testDag_sequentialChain() {
        // t1 -> t2 -> t3
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        EmptyTask t1 = new EmptyTask("s");
        EmptyTask t2 = new EmptyTask("s");
        EmptyTask t3 = new EmptyTask("s");
        ddlJob.addSequentialTasks(Arrays.asList(t1, t2, t3));

        DdlContext ddlContext = new DdlContext();
        DdlEngineRequester requester = buildRequester(ddlJob, ddlContext);
        requester.generateIds();

        String dag = ddlJob.visualizeTasks();
        long id1 = t1.getTaskId(), id2 = t2.getTaskId(), id3 = t3.getTaskId();

        Set<String> expected = new HashSet<>(Arrays.asList(
            expectedNode(id1),
            expectedNode(id2),
            expectedNode(id3),
            expectedEdge(id1, id2),
            expectedEdge(id2, id3)
        ));
        Assert.assertEquals(expected, dagContentLines(dag));
    }

    @Test
    public void testDag_forkJoin() {
        //   head
        //   / \
        //  b1  b2
        //   \ /
        //   tail
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        EmptyTask head = new EmptyTask("s");
        EmptyTask b1 = new EmptyTask("s");
        EmptyTask b2 = new EmptyTask("s");
        EmptyTask tail = new EmptyTask("s");

        ddlJob.addTask(head);
        ddlJob.addTask(b1);
        ddlJob.addTask(b2);
        ddlJob.addTask(tail);
        ddlJob.addTaskRelationship(head, b1);
        ddlJob.addTaskRelationship(head, b2);
        ddlJob.addTaskRelationship(b1, tail);
        ddlJob.addTaskRelationship(b2, tail);

        DdlContext ddlContext = new DdlContext();
        DdlEngineRequester requester = buildRequester(ddlJob, ddlContext);
        requester.generateIds();

        String dag = ddlJob.visualizeTasks();
        long hId = head.getTaskId(), b1Id = b1.getTaskId(), b2Id = b2.getTaskId(), tId = tail.getTaskId();

        Set<String> expected = new HashSet<>(Arrays.asList(
            expectedNode(hId),
            expectedNode(b1Id),
            expectedNode(b2Id),
            expectedNode(tId),
            expectedEdge(hId, b1Id),
            expectedEdge(hId, b2Id),
            expectedEdge(b1Id, tId),
            expectedEdge(b2Id, tId)
        ));
        Assert.assertEquals(expected, dagContentLines(dag));
    }

    @Test
    public void testDag_diamond() {
        //       t1
        //      / \
        //    t2   t3
        //      \ /
        //       t4
        //       |
        //       t5
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        EmptyTask t1 = new EmptyTask("s");
        EmptyTask t2 = new EmptyTask("s");
        EmptyTask t3 = new EmptyTask("s");
        EmptyTask t4 = new EmptyTask("s");
        EmptyTask t5 = new EmptyTask("s");

        ddlJob.addTask(t1);
        ddlJob.addTask(t2);
        ddlJob.addTask(t3);
        ddlJob.addTask(t4);
        ddlJob.addTask(t5);
        ddlJob.addTaskRelationship(t1, t2);
        ddlJob.addTaskRelationship(t1, t3);
        ddlJob.addTaskRelationship(t2, t4);
        ddlJob.addTaskRelationship(t3, t4);
        ddlJob.addTaskRelationship(t4, t5);

        DdlContext ddlContext = new DdlContext();
        DdlEngineRequester requester = buildRequester(ddlJob, ddlContext);
        requester.generateIds();

        String dag = ddlJob.visualizeTasks();
        long id1 = t1.getTaskId(), id2 = t2.getTaskId(), id3 = t3.getTaskId();
        long id4 = t4.getTaskId(), id5 = t5.getTaskId();

        // Verify all IDs are unique and in range [2, 6]
        Set<Long> ids = new HashSet<>(Arrays.asList(id1, id2, id3, id4, id5));
        Assert.assertEquals(5, ids.size());
        Assert.assertEquals(new HashSet<>(Arrays.asList(2L, 3L, 4L, 5L, 6L)), ids);

        // Verify DAG structure precisely
        Set<String> expected = new HashSet<>(Arrays.asList(
            expectedNode(id1),
            expectedNode(id2),
            expectedNode(id3),
            expectedNode(id4),
            expectedNode(id5),
            expectedEdge(id1, id2),
            expectedEdge(id1, id3),
            expectedEdge(id2, id4),
            expectedEdge(id3, id4),
            expectedEdge(id4, id5)
        ));
        Assert.assertEquals(expected, dagContentLines(dag));
    }

    @Test
    public void testDag_wideParallel() {
        //     head
        //   / | | \
        //  b1 b2 b3 b4
        //   \ | | /
        //     tail
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        EmptyTask head = new EmptyTask("s");
        EmptyTask b1 = new EmptyTask("s");
        EmptyTask b2 = new EmptyTask("s");
        EmptyTask b3 = new EmptyTask("s");
        EmptyTask b4 = new EmptyTask("s");
        EmptyTask tail = new EmptyTask("s");

        ddlJob.addTask(head);
        ddlJob.addTask(tail);
        for (EmptyTask b : Arrays.asList(b1, b2, b3, b4)) {
            ddlJob.addTask(b);
            ddlJob.addTaskRelationship(head, b);
            ddlJob.addTaskRelationship(b, tail);
        }

        DdlContext ddlContext = new DdlContext();
        DdlEngineRequester requester = buildRequester(ddlJob, ddlContext);
        requester.generateIds();

        String dag = ddlJob.visualizeTasks();
        long hId = head.getTaskId(), tId = tail.getTaskId();
        long b1Id = b1.getTaskId(), b2Id = b2.getTaskId();
        long b3Id = b3.getTaskId(), b4Id = b4.getTaskId();

        // 6 nodes + 8 edges = 14 content lines
        Set<String> content = dagContentLines(dag);
        Assert.assertEquals(14, content.size());

        // Verify all nodes present
        for (long id : Arrays.asList(hId, b1Id, b2Id, b3Id, b4Id, tId)) {
            Assert.assertTrue("Missing node " + id, content.contains(expectedNode(id)));
        }
        // Verify all edges: head -> each branch, each branch -> tail
        for (long bId : Arrays.asList(b1Id, b2Id, b3Id, b4Id)) {
            Assert.assertTrue("Missing edge head->" + bId, content.contains(expectedEdge(hId, bId)));
            Assert.assertTrue("Missing edge " + bId + "->tail", content.contains(expectedEdge(bId, tId)));
        }
    }

    @Test
    public void testDag_idempotentAcrossRuns() {
        // Verify that each call to generateIds produces the same IDs (always starts from 1)
        for (int run = 0; run < 3; run++) {
            ExecutableDdlJob ddlJob = new ExecutableDdlJob();
            EmptyTask task = new EmptyTask("s");
            ddlJob.addTask(task);

            DdlContext ddlContext = new DdlContext();
            DdlEngineRequester requester = buildRequester(ddlJob, ddlContext);
            requester.generateIds();

            Assert.assertEquals("Run " + run + ": jobId should be 1", 1L, ddlContext.getJobId());
            Assert.assertEquals("Run " + run + ": taskId should be 2", Long.valueOf(2L), task.getTaskId());

            String dag = ddlJob.visualizeTasks();
            Set<String> expected = new HashSet<>(Arrays.asList(expectedNode(2)));
            Assert.assertEquals("Run " + run + ": DAG mismatch", expected, dagContentLines(dag));
        }
    }
}
