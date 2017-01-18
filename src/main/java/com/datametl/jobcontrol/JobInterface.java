package com.datametl.jobcontrol;

/**
 * Created by mspallino on 1/16/17.
 */
interface JobInterface {
    boolean start();
    boolean stop();
    boolean restart();
    boolean isRunning();
    boolean kill();
    int getTaskReturnCode();
}
