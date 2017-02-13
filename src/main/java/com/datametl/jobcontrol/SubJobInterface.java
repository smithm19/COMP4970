package com.datametl.jobcontrol;

import org.json.JSONObject;

/**
 * Created by mspallino on 1/16/17.
 */
public interface SubJobInterface {
    boolean start();
    boolean stop();
    boolean restart();
    boolean isRunning();
    boolean kill();
    JobState getTaskReturnCode();
    Job getParent();
    void setParent(Job parent);
    JSONObject getETLPacketFromParent();
}
