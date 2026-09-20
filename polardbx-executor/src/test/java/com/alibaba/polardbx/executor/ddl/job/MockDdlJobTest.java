package com.alibaba.polardbx.executor.ddl.job;

import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class MockDdlJobTest {

    @Test
    public void testGenerateRandomDag() {
        // covers generateRandomDag() and the DdlJobManager.ID_GENERATOR.nextId() call inside it
        MockDdlJob mockDdlJob = new MockDdlJob(3, 2, 50, false);
        ExecutableDdlJob job = mockDdlJob.create();
        Assert.assertNotNull(job);
    }

    @Test
    public void testGenerateRandomDagWithSubJob() {
        MockDdlJob mockDdlJob = new MockDdlJob(3, 2, 50, true);
        ExecutableDdlJob job = mockDdlJob.create();
        Assert.assertNotNull(job);
    }

    @Test
    public void testExcludeResources() {
        // covers excludeResources() and the DdlJobManager.ID_GENERATOR.nextId() call inside it
        MockDdlJob mockDdlJob = new MockDdlJob(3, 2, 50, false);
        Set<String> resources = new HashSet<>();
        mockDdlJob.excludeResources(resources);
        Assert.assertEquals(1, resources.size());
        Assert.assertTrue(resources.iterator().next().startsWith("mock_resource_"));
    }

    @Test
    public void testExcludeResourcesWithSubJob() {
        // mockSubJob=true skips resource generation
        MockDdlJob mockDdlJob = new MockDdlJob(3, 2, 50, true);
        Set<String> resources = new HashSet<>();
        mockDdlJob.excludeResources(resources);
        Assert.assertTrue(resources.isEmpty());
    }

    @Test
    public void testGenerateSequentialDag() {
        // covers generateSequentialDag() and the DdlJobManager.ID_GENERATOR.nextId() call inside it
        ExecutableDdlJob job = MockDdlJob.generateSequentialDag(3, false);
        Assert.assertNotNull(job);
    }

    @Test
    public void testGenerateSequentialDagWithSubJob() {
        ExecutableDdlJob job = MockDdlJob.generateSequentialDag(3, true);
        Assert.assertNotNull(job);
    }
}
