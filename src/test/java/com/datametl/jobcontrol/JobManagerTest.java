package com.datametl.jobcontrol;

import com.datametl.tasks.ExampleTask;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import static org.junit.Assert.*;

/**
 * Created by mspallino on 2/2/17.
 */
public class JobManagerTest {

    private Vector<SubJob> subJobs;
    private Job job;

    @Before
    public void setUp() throws Exception {
        subJobs = new Vector<SubJob>();
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

    @Test
    public void  getETLPacket() throws Exception {
        JobManager manager = new JobManager();
        UUID jobId = manager.addJob(job);

        JSONObject packet = job.getETLPacket();
        System.out.println(packet);

        assertNotNull(packet.getJSONArray("data"));
        assertNotNull(packet.get("source"));
        assertNotNull(packet.get("rules"));
        assertNotNull(packet.get("destination"));

        JSONObject managerPacket = manager.getJobETLPacket(jobId);
        System.out.println(managerPacket);

        assertNotNull(managerPacket.getJSONArray("data"));
        assertNotNull(managerPacket.get("source"));
        assertNotNull(managerPacket.get("rules"));
        assertNotNull(managerPacket.get("destination"));
    }

}