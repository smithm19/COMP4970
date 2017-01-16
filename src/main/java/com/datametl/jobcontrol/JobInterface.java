package com.datametl.jobcontrol;

/**
 * Created by mspallino on 1/16/17.
 */
public interface JobInterface {
    boolean start();
    boolean stop();
    boolean restart();
    boolean isRunning();
}
