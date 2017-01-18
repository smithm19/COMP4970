package com.datametl.jobcontrol;

import com.datametl.tasks.Task;

/**
 * Created by mspallino on 1/16/17.
 */
class Job implements JobInterface, Runnable {

    private Task t;
    private Thread curThread;

    Job(Task t) {
        this.t = t;
        curThread = new Thread(this);
    }

    public boolean start() {
        curThread.start();
        return true;
    }

    public boolean stop() {
        try {
            curThread.join();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean restart() {
        stop();
        start();
        return true;
    }

    public boolean kill() {
        curThread.interrupt();
        return true;
    }

    public int getTaskReturnCode() {
        if (!isRunning()) {
            return t.getResult();
        } else {
            stop();
            return t.getResult();
        }
    }

    public boolean isRunning() {
        return curThread.isAlive();
    }

    public void run() {
        t.apply();
    }
}
