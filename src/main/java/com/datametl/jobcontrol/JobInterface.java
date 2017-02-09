package com.datametl.jobcontrol;

import org.json.JSONObject;

import java.util.List;
import java.util.Map;

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
    JSONObject getETLPacket();
    void setETLPacket(JSONObject packet);
}
