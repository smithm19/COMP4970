package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.jobcontrol.Job;

/**
 * Created by mspallino on 1/18/17.
 */
public class ExampleTask implements Task {

    private JobState returnCode;
    private SubJob parent = null;

    /**
     * This is just meant to be an example of how this "should" work.
     */
    public void apply() {
        try {
            Thread.sleep(2000);
        } catch (Exception ex) {
            ex.printStackTrace();
            returnCode = JobState.KILLED;
            return;
        }
        System.out.println("Did the thing!");

        if (parent != null) {
            System.out.println("Got my parent's ETL packet!: " + parent.getETLPacketFromParent());
        }
        returnCode = JobState.SUCCESS;
    }

    public JobState getResult() {
        return returnCode;
    }

    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    public SubJob getParent() {
        return parent;
    }
}
