package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import org.json.JSONObject;

import java.io.File;

/**
 * Created by mspallino on 2/16/17.
 */
public class DataSegmentationTask implements Task {
    private SubJob parent;
    private int documentsPerChunk;
    private long currentBytePosition;
    private long maxFilePosition;

    public DataSegmentationTask(int documentsPerChunk) {
        this.documentsPerChunk = documentsPerChunk;
        this.currentBytePosition = 0;
        this.maxFilePosition = 0;
    }

    public void apply() {
        JSONObject etlPacket = parent.getETLPacketFromParent();
        String filePath = etlPacket.getJSONObject("source").getString("path");
        File fin = new File(filePath);
        maxFilePosition = fin.length();
        etlPacket.put("documents_to_read", documentsPerChunk);
        etlPacket.put("currentBytePosition", currentBytePosition);
        
        Task extractTask = new ExtractTask();
        SubJob newExtractJob = new SubJob(extractTask);
        parent.getParent().addSubJob(newExtractJob);

        /*
         * We want to keep checking to see if the current byte position from the packet has changed.
         * If it has, that means that the last Extract Job we created is done and we are ready to make a new one.
         * If it has not, we should keep sleeping until it has.
         */
        long packetBytePosition;
        while(currentBytePosition < maxFilePosition) {
            packetBytePosition = etlPacket.getLong("currentBytePosition");
            if(currentBytePosition == packetBytePosition) {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                continue;
            }
            currentBytePosition = packetBytePosition;
            extractTask = new ExtractTask();
            newExtractJob = new SubJob(extractTask);
            parent.getParent().addSubJob(newExtractJob);
        }

    }

    public JobState getResult() {
        return null;
    }

    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    public SubJob getParent() {
        return parent;
    }
}
