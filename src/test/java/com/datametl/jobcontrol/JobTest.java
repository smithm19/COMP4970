package com.datametl.jobcontrol;

import com.datametl.tasks.ExampleTask;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by mspallino on 1/16/17.
 */
public class JobTest {
    private ExampleTask exampleTask;

    @Before
    public void setUp() throws Exception {
        this.exampleTask = new ExampleTask();
    }

    @Test
    public void start() throws Exception {
        Job test = new Job(this.exampleTask);
        boolean result = test.start();
        assertTrue(result);
        assertTrue(test.isRunning());
        test.kill();
    }

    @Test
    public void stop() throws Exception {
        Job test = new Job(this.exampleTask);
        boolean result = test.stop();
        assertTrue(result);
        assertFalse(test.isRunning());
        test.kill();
    }

    @Test
    public void restart() throws Exception {
        Job test = new Job(this.exampleTask);
        boolean result = test.restart();
        assertTrue(result);
        assertTrue(test.isRunning());
        test.kill();
    }

    @Test
    public void isRunning() throws Exception {
        Job test = new Job(this.exampleTask);
        assertFalse(test.isRunning());
        test.start();
        assertTrue(test.isRunning());
        test.stop();
        assertFalse(test.isRunning());
        test.kill();
    }

    @Test
    public void successfulExecution() throws Exception {
        Job test = new Job(this.exampleTask);
        test.start();
        assertEquals(0, test.getTaskReturnCode());
    }

    @Test
    public void badExecution() throws Exception {
        Job test = new Job(this.exampleTask);
        test.start();
        test.kill();
        assertEquals(-1, test.getTaskReturnCode());
    }

}