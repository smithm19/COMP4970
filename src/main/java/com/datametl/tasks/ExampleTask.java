package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;

/**
 * Created by mspallino on 1/18/17.
 */
public class ExampleTask implements Task {

    private JobState returnCode;

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
        returnCode = JobState.SUCCESS;
    }

    public JobState getResult() {
        return returnCode;
    }
}
