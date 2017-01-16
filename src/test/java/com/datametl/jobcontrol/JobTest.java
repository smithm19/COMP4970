package com.datametl.jobcontrol;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by mspallino on 1/16/17.
 */
public class JobTest {

    @Test
    public void start() throws Exception {
        Job test = new Job();
        boolean result = test.start();
        assertTrue(result);
        assertTrue(test.isRunning());
    }

    @Test
    public void stop() throws Exception {
        Job test = new Job();
        boolean result = test.stop();
        assertTrue(result);
        assertFalse(test.isRunning());
    }

    @Test
    public void restart() throws Exception {
        Job test = new Job();
        boolean result = test.restart();
        assertTrue(result);
        assertTrue(test.isRunning());
    }

    @Test
    public void isRunning() throws Exception {
        Job test = new Job();
        assertFalse(test.isRunning());
        test.start();
        assertTrue(test.isRunning());
        test.stop();
        assertFalse(test.isRunning());
    }


}