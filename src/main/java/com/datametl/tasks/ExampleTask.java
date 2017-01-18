package com.datametl.tasks;

import java.util.*;

/**
 * Created by mspallino on 1/18/17.
 */
public class ExampleTask implements Task {

    private int returnCode;

    /**
     * This is just meant to be an example of how this "should" work.
     */
    public void apply() {
        try {
            Thread.sleep(2000);
        } catch (Exception ex) {
            ex.printStackTrace();
            returnCode = -1;
            return;
        }
        System.out.println("Did the thing!");
        returnCode = 0;
    }

    public int getResult() {
        return returnCode;
    }
}
