package com.datametl.jobcontrol;

import com.datametl.tasks.ExampleTask;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by mspallino on 2/2/17.
 */
public class JobManagerTest {

    private List<SubJob> subJobs;
    private Job job;

    @Before
    public void setUp() throws Exception {
        subJobs = new ArrayList<SubJob>();
        for(int i = 0; i < 3; ++i) {
            subJobs.add(new SubJob(new ExampleTask()));
        }

        this.job = new Job(this.subJobs, 3);
    }

    @Test
    public void addJob() throws Exception {
        JobManager manager = new JobManager();
        UUID jobId = manager.addJob(job);
        boolean started = manager.startJob(jobId);
        assertTrue(started);

        setUp();
        UUID newJobId = manager.addJob(job);
        boolean newJobStarted = manager.startJob(newJobId);
        assertTrue(newJobStarted);

        Thread.sleep(1000);

        manager.stopJob(jobId);
        manager.stopJob(newJobId);
        Thread.sleep(2000);
    }

    @Test
    public void removeJob() throws Exception {
        JobManager manager = new JobManager();
        UUID jobId = manager.addJob(job);
        boolean started = manager.startJob(jobId);
        assertTrue(started);
        manager.removeJob(jobId);
    }

}