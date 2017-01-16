package com.datametl.jobcontrol;

/**
 * Created by mspallino on 1/16/17.
 */
public class Job implements JobInterface {

    private boolean running;

    public Job() {
        running = false;
    }

    public boolean start() {
        running = true;
        return true;
    }

    public boolean stop() {
        running = false;
        return true;
    }

    public boolean restart() {
        stop();
        start();
        return true;
    }

    public boolean isRunning() {
        return running;
    }
}
