package com.datametl.jobcontrol;

import com.datametl.tasks.ExampleTask;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by mspallino on 1/23/17.
 */
public class JobTest {
    private List<SubJob> subJobs;

    @Before
    public void setUp() {
        subJobs = new ArrayList<SubJob>();
        for(int i = 0; i < 3; ++i) {
            subJobs.add(new SubJob(new ExampleTask()));
        }
    }

    @Test
    public void run() throws Exception {
        Job job = new Job(this.subJobs, 3);
        job.start();
    }

    @Test
    public void start() throws Exception {
        Job job = new Job(this.subJobs, 3);
        boolean val = job.start();
        assertTrue(val);
        assertEquals(JobState.RUNNING, job.getState());
        assertTrue(job.isRunning());
        job.kill();
    }

    @Test
    public void stop() throws Exception {
        Job job = new Job(this.subJobs, 3);
        job.start();
        boolean val = job.stop();
        assertTrue(val);
        assertEquals(JobState.SUCCESS, job.getState());
        job.kill();
    }

    @Test
    public void restart() throws Exception {
        Job job = new Job(this.subJobs, 3);
        boolean val = job.restart();
        assertTrue(val);
        assertEquals(JobState.RUNNING, job.getState());
        job.kill();
    }


    @Test
    public void kill() throws Exception {
        Job job = new Job(this.subJobs, 3);
        job.start();
        boolean val = job.kill();
        assertTrue(val);
        assertEquals(JobState.KILLED, job.getState());
    }

    @Test
    public void addSubJob() throws Exception {
        Job job = new Job(this.subJobs, 3);
        job.start();
        assertEquals(3, job.getSubJobs().size());
        Thread.sleep(2000);
        SubJob newSubJob = new SubJob(new ExampleTask());
        job.addSubJob(newSubJob);
        assertEquals(4, job.getSubJobs().size());
        job.stop();
        List<SubJob> subJobs = job.getSubJobs();
        for (SubJob s: subJobs) {
            assertEquals(JobState.SUCCESS, s.getTaskReturnCode());
        }
        job.kill();
    }

    @Test
    public void goodJobExecution() throws Exception {
        Job job = new Job(this.subJobs, 3);
        boolean val1 = job.start();
        assertTrue(val1);

        boolean val2 = job.stop();
        assertTrue(val2);

        assertEquals(JobState.SUCCESS, job.getState());
    }

}