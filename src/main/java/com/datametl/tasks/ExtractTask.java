package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import org.json.JSONObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by TseAndy on 2/6/17.
 */
public class ExtractTask implements Task{

    private JobState returnCode;
    private SubJob parent = null;
    private Map<String,Collection<String>> table = new HashMap<String, Collection<String>>();

    public void apply() {

        JSONObject etlPacket = parent.getETLPacketFromParent();
        String filePath = etlPacket.getJSONObject("source").getString("path");
        String docToRead = etlPacket.getJSONObject("documents_to_read").toString();


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
