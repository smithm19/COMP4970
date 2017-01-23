package com.datametl.jobcontrol;

import java.util.List;

/**
 * Created by mspallino on 1/23/17.
 */
public class Job implements JobInterface, Runnable {

    private List<SubJob> subJobs;
    private int retries;
    private JobState state;
    private Thread curThread;
    private SubJob curSubJob;

    public Job(List<SubJob> subJobs, int retries) {
        this.retries = retries;
        this.subJobs = subJobs;
        this.state = JobState.NOT_STARTED;
        this.curThread = new Thread(this);
        curSubJob = subJobs.get(0);
    }

    public void run() {
        int currentRetryCount = 0;
        for(SubJob sub: subJobs) {
            curSubJob = sub;
            sub.start();
            sub.stop();

            while(currentRetryCount < retries) {
                JobState returnState = sub.getTaskReturnCode();

                if (returnState == JobState.FAILED || returnState == JobState.KILLED) {
                    sub.restart();
                    state = JobState.FAILED;
                } else {
                    break;
                }
            }

            currentRetryCount = 0;
            if (state == JobState.FAILED || state == JobState.KILLED) {
                break;
            }

        }

    }

    public boolean start() {
        state = JobState.RUNNING;
        curThread.start();
        return true;
    }

    public boolean stop() {
        try {
            curThread.join();
            state = JobState.SUCCESS;
        } catch (InterruptedException ex) {
            state = JobState.FAILED;
            return false;
        }
        return true;
    }

    public boolean restart() {
        stop();
        start();
        return true;
    }

    public boolean isRunning() {
        return state == JobState.RUNNING;
    }

    public boolean kill() {
        curSubJob.kill();
        curThread.interrupt();
        state = JobState.KILLED;
        return true;
    }

    public List<SubJob> getSubJobs() {
        return subJobs;
    }

    public boolean addSubJob(SubJob sub) {
        return subJobs.add(sub);
    }

    public JobState getState() {
        return state;
    }
}
