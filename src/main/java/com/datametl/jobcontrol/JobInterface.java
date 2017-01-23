package com.datametl.jobcontrol;

import java.util.List;

/**
 * Created by mspallino on 1/23/17.
 */
public interface JobInterface {
    boolean start();
    boolean stop();
    boolean restart();
    boolean isRunning();
    boolean kill();
    List<SubJob> getSubJobs();
    boolean addSubJob(SubJob sub);
    JobState getState();
}
