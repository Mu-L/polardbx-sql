package com.alibaba.polardbx.server;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit test for TddlLauncher
 */
public class TddlLauncherTest {

    private Thread testThread;
    private AtomicBoolean threadStarted;
    private AtomicBoolean threadInterrupted;
    private AtomicReference<Throwable> threadException;

    @Before
    public void setUp() {
        testThread = null;
        threadStarted = new AtomicBoolean(false);
        threadInterrupted = new AtomicBoolean(false);
        threadException = new AtomicReference<>();
    }

    @After
    public void tearDown() throws InterruptedException {
        // Clear system property after each test
        System.clearProperty("java.lang.VirtualThreadConfig.autoVirtualTransition");

        if (testThread != null && testThread.isAlive()) {
            testThread.interrupt();
            testThread.join(1000);
        }
    }

    /**
     * Test that waitForever blocks the thread indefinitely until interrupted when autoVirtualTransition is true
     */
    @Test(timeout = 5000)
    public void testWaitForeverBlocksThreadWhenAutoVirtualTransitionEnabled() throws InterruptedException {
        // Enable autoVirtualTransition
        System.setProperty("java.lang.VirtualThreadConfig.autoVirtualTransition", "true");

        CountDownLatch latch = new CountDownLatch(1);

        testThread = new Thread(() -> {
            try {
                threadStarted.set(true);
                latch.countDown();
                TddlLauncher.waitForever();
            } catch (Throwable e) {
                threadException.set(e);
            }
        });

        testThread.start();

        // Wait for thread to start and enter waitForever
        Assert.assertTrue("Thread should start within timeout", latch.await(1, TimeUnit.SECONDS));
        Assert.assertTrue("Thread should have started", threadStarted.get());

        // Give some time for thread to enter wait state
        Thread.sleep(100);

        // Thread should still be alive and waiting
        Assert.assertTrue("Thread should still be alive", testThread.isAlive());
        Assert.assertNull("No exception should have been thrown", threadException.get());
    }

    /**
     * Test that interrupting the thread causes waitForever to handle InterruptedException
     * and restore the interrupted status when autoVirtualTransition is enabled
     */
    @Test(timeout = 5000)
    public void testWaitForeverHandlesInterruptionWhenEnabled() throws InterruptedException {
        // Enable autoVirtualTransition
        System.setProperty("java.lang.VirtualThreadConfig.autoVirtualTransition", "true");

        CountDownLatch latch = new CountDownLatch(1);

        testThread = new Thread(() -> {
            try {
                threadStarted.set(true);
                latch.countDown();
                TddlLauncher.waitForever();
                // After waitForever returns due to interruption, check interrupt status
                threadInterrupted.set(Thread.currentThread().isInterrupted());
            } catch (Throwable e) {
                threadException.set(e);
            }
        });

        testThread.start();

        // Wait for thread to start and enter waitForever
        Assert.assertTrue("Thread should start within timeout", latch.await(1, TimeUnit.SECONDS));

        // Give some time for thread to enter wait state
        Thread.sleep(100);

        // Interrupt the thread
        testThread.interrupt();

        // Wait for thread to finish
        testThread.join(2000);

        // Verify thread has finished
        Assert.assertFalse("Thread should have terminated", testThread.isAlive());

        // Verify interrupted status was restored
        Assert.assertTrue("Thread interrupted status should be restored", threadInterrupted.get());

        // Verify no unexpected exception
        Assert.assertNull("No unexpected exception should have been thrown", threadException.get());
    }

    /**
     * Test that multiple interruptions are handled correctly when autoVirtualTransition is enabled
     */
    @Test(timeout = 5000)
    public void testWaitForeverMultipleInterruptionsWhenEnabled() throws InterruptedException {
        // Enable autoVirtualTransition
        System.setProperty("java.lang.VirtualThreadConfig.autoVirtualTransition", "true");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean interruptedOnce = new AtomicBoolean(false);

        testThread = new Thread(() -> {
            try {
                threadStarted.set(true);
                latch.countDown();
                TddlLauncher.waitForever();
                interruptedOnce.set(Thread.currentThread().isInterrupted());
            } catch (Throwable e) {
                threadException.set(e);
            }
        });

        testThread.start();

        // Wait for thread to start
        Assert.assertTrue("Thread should start within timeout", latch.await(1, TimeUnit.SECONDS));

        // Give some time for thread to enter wait state
        Thread.sleep(100);

        // Interrupt multiple times
        testThread.interrupt();
        Thread.sleep(50);
        testThread.interrupt();

        // Wait for thread to finish
        testThread.join(2000);

        // Verify thread handled interruption correctly
        Assert.assertFalse("Thread should have terminated", testThread.isAlive());
        Assert.assertTrue("Interrupted status should be set", interruptedOnce.get());
        Assert.assertNull("No unexpected exception", threadException.get());
    }

    /**
     * Test that waitForever doesn't consume excessive CPU when autoVirtualTransition is enabled
     */
    @Test(timeout = 5000)
    public void testWaitForeverDoesNotConsumeCPUWhenEnabled() throws InterruptedException {
        // Enable autoVirtualTransition
        System.setProperty("java.lang.VirtualThreadConfig.autoVirtualTransition", "true");

        CountDownLatch latch = new CountDownLatch(1);

        testThread = new Thread(() -> {
            try {
                threadStarted.set(true);
                latch.countDown();
                TddlLauncher.waitForever();
            } catch (Throwable e) {
                threadException.set(e);
            }
        });

        testThread.start();

        // Wait for thread to start
        Assert.assertTrue("Thread should start within timeout", latch.await(1, TimeUnit.SECONDS));

        // Let it wait for a bit
        Thread.sleep(500);

        // Thread should be in WAITING or TIMED_WAITING state, not RUNNABLE
        Thread.State state = testThread.getState();
        Assert.assertTrue(
            "Thread should be in WAITING or TIMED_WAITING state, but was: " + state,
            state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING
        );

        // Clean up
        testThread.interrupt();
        testThread.join(1000);
    }

    /**
     * Test that waitForever returns immediately when autoVirtualTransition is not set
     */
    @Test(timeout = 5000)
    public void testWaitForeverReturnsImmediatelyWhenNotSet() throws InterruptedException {
        // Do not set the property (it should be null by default)
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean methodReturned = new AtomicBoolean(false);

        testThread = new Thread(() -> {
            try {
                threadStarted.set(true);
                latch.countDown();
                TddlLauncher.waitForever();
                methodReturned.set(true);
            } catch (Throwable e) {
                threadException.set(e);
            }
        });

        testThread.start();

        // Wait for thread to start
        Assert.assertTrue("Thread should start within timeout", latch.await(1, TimeUnit.SECONDS));

        // Wait a bit and verify method returned immediately
        testThread.join(1000);

        Assert.assertFalse("Thread should have terminated", testThread.isAlive());
        Assert.assertTrue("Method should have returned immediately", methodReturned.get());
        Assert.assertNull("No exception should have been thrown", threadException.get());
    }

    /**
     * Test that waitForever returns immediately when autoVirtualTransition is set to false
     */
    @Test(timeout = 5000)
    public void testWaitForeverReturnsImmediatelyWhenSetToFalse() throws InterruptedException {
        System.setProperty("java.lang.VirtualThreadConfig.autoVirtualTransition", "false");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean methodReturned = new AtomicBoolean(false);

        testThread = new Thread(() -> {
            try {
                threadStarted.set(true);
                latch.countDown();
                TddlLauncher.waitForever();
                methodReturned.set(true);
            } catch (Throwable e) {
                threadException.set(e);
            }
        });

        testThread.start();

        // Wait for thread to start
        Assert.assertTrue("Thread should start within timeout", latch.await(1, TimeUnit.SECONDS));

        // Wait a bit and verify method returned immediately
        testThread.join(1000);

        Assert.assertFalse("Thread should have terminated", testThread.isAlive());
        Assert.assertTrue("Method should have returned immediately", methodReturned.get());
        Assert.assertNull("No exception should have been thrown", threadException.get());
    }

    /**
     * Test that waitForever returns immediately when autoVirtualTransition is set to an invalid value
     */
    @Test(timeout = 5000)
    public void testWaitForeverReturnsImmediatelyWhenSetToInvalidValue() throws InterruptedException {
        System.setProperty("java.lang.VirtualThreadConfig.autoVirtualTransition", "invalid");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean methodReturned = new AtomicBoolean(false);

        testThread = new Thread(() -> {
            try {
                threadStarted.set(true);
                latch.countDown();
                TddlLauncher.waitForever();
                methodReturned.set(true);
            } catch (Throwable e) {
                threadException.set(e);
            }
        });

        testThread.start();

        // Wait for thread to start
        Assert.assertTrue("Thread should start within timeout", latch.await(1, TimeUnit.SECONDS));

        // Wait a bit and verify method returned immediately
        testThread.join(1000);

        Assert.assertFalse("Thread should have terminated", testThread.isAlive());
        Assert.assertTrue("Method should have returned immediately", methodReturned.get());
        Assert.assertNull("No exception should have been thrown", threadException.get());
    }

    /**
     * Test that waitForever is case-insensitive for the property value
     */
    @Test(timeout = 5000)
    public void testWaitForeverIsCaseInsensitive() throws InterruptedException {
        // Test with "TRUE" (uppercase)
        System.setProperty("java.lang.VirtualThreadConfig.autoVirtualTransition", "TRUE");

        CountDownLatch latch = new CountDownLatch(1);

        testThread = new Thread(() -> {
            try {
                threadStarted.set(true);
                latch.countDown();
                TddlLauncher.waitForever();
            } catch (Throwable e) {
                threadException.set(e);
            }
        });

        testThread.start();

        // Wait for thread to start
        Assert.assertTrue("Thread should start within timeout", latch.await(1, TimeUnit.SECONDS));

        // Give some time for thread to enter wait state
        Thread.sleep(100);

        // Thread should still be alive and waiting (because "TRUE" should be treated as true)
        Assert.assertTrue("Thread should still be alive", testThread.isAlive());
        Assert.assertNull("No exception should have been thrown", threadException.get());

        // Clean up
        testThread.interrupt();
        testThread.join(1000);
    }
}
