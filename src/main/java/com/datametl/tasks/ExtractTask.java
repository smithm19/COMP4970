package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import org.json.JSONObject;

import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by TseAndy on 2/6/17.
 */
public class ExtractTask implements Task{

    private JobState returnCode = JobState.NOT_STARTED;
    private SubJob parent = null;
    private Map<String,Collection<String>> table = new HashMap<String, Collection<String>>();

    public void apply() {
        returnCode = JobState.RUNNING;
        JSONObject etlPacket = parent.getETLPacketFromParent();
        String filePath = etlPacket.getJSONObject("source").getString("path");
        int docToRead = etlPacket.getInt("documents_to_read");
        long bytePos = etlPacket.getLong("current_byte_position");

        // TODO: This is just a place holder to pretend that this is doing work, really read the file here.
        try {
            System.out.println("working on file... " + filePath);
            Thread.sleep(2000);
        } catch(InterruptedException ex) {
            ex.printStackTrace();
            returnCode = JobState.FAILED;
        }
        etlPacket.put("current_byte_position", bytePos + 20);
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

    private void dataExtract(){



        return;
    }
}
