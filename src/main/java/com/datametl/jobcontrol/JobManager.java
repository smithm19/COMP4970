package com.datametl.jobcontrol;

import java.util.*;
import org.json.*;


/**
 * Created by mspallino on 1/30/17.
 */
public class JobManager implements Runnable {

    private Map<UUID, Job> jobs;
    private Thread curThread;

    public JobManager() {
        jobs = new HashMap<UUID, Job>();
        curThread = new Thread(this);
        curThread.start();
    }

    public UUID addJob(Job newJob) {
        UUID newId = UUID.randomUUID();
        String emptyPacketData = "{\n" +
                "    \"source\": {\n" +
                "        \"host_ip\": \"\",\n" +
                "        \"host_port\": 1234,\n" +
                "        \"path\": \"\",\n" +
                "        \"file_type\": \"\"\n" +
                "    },\n" +
                "    \"rules\": {\n" +
                "        \"transformations\": {\n" +
                "            \"transform1\": {\n" +
                "                \"source_column\": \"test\",\n" +
                "                \"new_field\": \"test\",\n" +
                "                \"transform\": \"test\"\n" +
                "            },\n" +
                "            \"transform2\": {\n" +
                "                \"source_column\": \"age\",\n" +
                "                \"new_field\": null,\n" +
                "                \"transform\": \"MULT 2\"\n" +
                "            },\n" +
                "            \"transform3\": {\n" +
                "                \"source_column\": \"age\",\n" +
                "                \"new_field\": \"desty4\",\n" +
                "                \"transform\": \"MULT 4\"\n" +
                "            }\n" +
                "        },\n" +
                "        \"mappings\": {\n" +
                "            \"tester1\": \"desty2\",\n" +
                "            \"tester2\": \"desty1\"\n" +
                "        },\n" +
                "        \"filters\": {\n" +
                "            \"filter1\": {\n" +
                "                \"source_column\": \"tester4\",\n" +
                "                \"filter_value\": \"tester\",\n" +
                "                \"equality_test\": \"eq\"\n" +
                "            }\n" +
                "        }\n" +
                "    },\n" +
                "    \"destination\": {\n" +
                "        \"host_ip\": \"\",\n" +
                "        \"host_port\": 1234,\n" +
                "        \"username\": \"\",\n" +
                "        \"password\": \"\",\n" +
                "        \"storage_type\": \"\"\n" +
                "    },\n" +
                "    \"data\": {\n" +
                "        \"source_header\": [tester1, tester2, age],\n" +
                "        \"destination_header\": [tester1, tester2, tester3, desty4],\n" +
                "        \"contents\": [[\"Matt\", \"yes\", \"22\"], [\"Andy\", \"no\", \"18\"]] \n" +
                "}}\n";

        JSONObject etlPacket = new JSONObject(emptyPacketData);
        newJob.setETLPacket(etlPacket);
        jobs.put(newId, newJob);
        return newId;
    }

    public void removeJob(UUID oldJobId) {
        jobs.remove(oldJobId);
    }

    public boolean startJob(UUID jobId) {
        return jobs.get(jobId).start();
    }

    public boolean killJob(UUID jobId) {
        return jobs.get(jobId).kill();
    }

    public boolean stopJob(UUID jobId) {
        return jobs.get(jobId).stop();
    }

    public JSONObject getJobETLPacket(UUID jobId) {
        return jobs.get(jobId).getETLPacket();
    }

    public void run() {
        int jobIndex = 0;
        int jobSize = jobs.size();
        List<UUID> jobIds = new ArrayList<UUID>();
        jobIds.addAll(jobs.keySet());
        while (true) {
            if (jobSize == 0 || jobIndex > jobSize) {
                System.out.println("No jobs...");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    System.out.println("Uh-oh");
                }
                jobSize = jobs.size();
                jobIds.clear();
                jobIds.addAll(jobs.keySet());
                continue;
            }
            UUID curJobId = jobIds.get(jobIndex);
            Job curJob = jobs.get(curJobId);
            JobState curState = curJob.getState();
            //TODO: update UI with job state info
            switch (curState) {
                case FAILED:
                    System.out.println("Job [" + curJobId + "] STATE: " + curState + " [RED]");
                    break;
                case KILLED:
                    System.out.println("Job [" + curJobId + "] STATE: " + curState + "  [BLACK]");
                    break;
                case NOT_STARTED:
                    System.out.println("Job [" + curJobId + "] STATE: " + curState + "  [GRAY]");
                    break;
                case RUNNING:
                    System.out.println("Job [" + curJobId + "] STATE: " + curState + "  [YELLOW]");
                    break;
                case SUCCESS:
                    System.out.println("Job [" + curJobId + "] STATE: " + curState + "  [GREEN]");
                    break;
                default:
                    break;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                System.out.println("Uh-oh");
            }
            jobIndex++;
            jobSize = jobs.size();
            jobIds.clear();
            jobIds.addAll(jobs.keySet());
            if (jobIndex == jobSize) {
                jobIndex = 0;
            }
        }
    }
}
