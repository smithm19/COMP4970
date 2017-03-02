package com.datametl.tasks;

import com.datametl.jobcontrol.Job;
import com.datametl.jobcontrol.JobManager;
import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.PrintWriter;
import java.util.UUID;
import java.util.Vector;

import static org.junit.Assert.*;

/**
 * Created by mspallino on 2/24/17.
 */
public class WorkflowTest {

    @Test
    public void run() throws Exception {
//        File f = new File("test.csv");
//        String data = "\"fName\",\"lName\",\"age\"\n\"mike\",\"spall\",\"22\"\n";
//        f.createNewFile();
//        BufferedWriter bw = new BufferedWriter(new PrintWriter(f));
//        bw.write(data);
//        bw.close();

        Vector<SubJob> subJobs = new Vector<SubJob>();
        Task dst = new DataSegmentationTask(250);
        SubJob dstSubJob = new SubJob(dst);
        subJobs.add(dstSubJob);
        Job job = new Job(subJobs, 3);
        JobManager manager = new JobManager();
        UUID jobId = manager.addJob(job);

        manager.startJob(jobId);
        manager.stopJob(jobId);

        Thread.sleep(2000);
        assertEquals(job.getState(), JobState.SUCCESS);

//        f.delete();
    }

}