package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;

/**
 * Created by mspallino on 1/18/17.
 */
public interface Task {
    void apply();
    JobState getResult();
}
