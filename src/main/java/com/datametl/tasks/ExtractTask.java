package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.opencsv.CSVReader;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

/**
 * Created by TseAndy on 2/6/17.
 */
public class ExtractTask implements Task{

    private JobState returnCode = JobState.NOT_STARTED;
    private SubJob parent = null;
    private JSONObject etlPacket;

    private ArrayList<String> fieldName = new ArrayList<String>();
    private List<List<String>> rows = new ArrayList<List<String>>();

    private String filePath;
    private String fileType;
    private int docToRead;
    private long bytePos;

    public ExtractTask() {
    }

    public void apply() {
        this.returnCode = JobState.RUNNING;
        this.etlPacket = parent.getETLPacketFromParent();
        this.filePath = etlPacket.getJSONObject("source").getString("path");
        this.fileType = etlPacket.getJSONObject("source").getString("file_type");
        this.docToRead = etlPacket.getInt("documents_to_read");
        this.bytePos = etlPacket.getLong("current_byte_position");

        if(this.fileType.equals("csv")){
            extractCSV();
        }else if(this.fileType.equals("json")){
            extractJSON();
        }else if(this.fileType.equals("xml")){
            extractXML();
        }else{
            this.returnCode = JobState.FAILED;
        }

        /*// TODO: This is just a place holder to pretend that this is doing work, really read the file here.
        try {
            System.out.println("working on file... " + filePath);
            Thread.sleep(2000);
        } catch(InterruptedException ex) {
            ex.printStackTrace();
            returnCode = JobState.FAILED;
        }
        etlPacket.put("current_byte_position", bytePos + 20);
        returnCode = JobState.SUCCESS;*/


    }

    public JobState getResult() {
        return this.returnCode;
    }

    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    public SubJob getParent() {
        return parent;
    }

    private void extractCSV() {

        int count=0;

        try{

            CSVReader reader = new CSVReader(new FileReader(this.filePath));
            String[] nextLine;

            //Each nextLine is a row in CSV
            while((nextLine=reader.readNext())!=null&&count<this.docToRead){

//                System.out.println("TESTING SHIT HERE");
//                for(String part:nextLine)
//                System.out.print(part+",");

                if(count==0) {
                    //First line must be field names
                    this.fieldName = new ArrayList<String>(Arrays.asList(nextLine));
                }

                this.rows.add(Arrays.asList(nextLine));
                count++;
            }

            reader.close();
            inputETLPacket(); //Puts information to ETLPacket

        }catch(IOException e){
            e.printStackTrace();
            this.returnCode = JobState.FAILED;
        }

        readContent();

        //TODO: Send to Matt



//        this.etlPacket.put("current_byte_position", bytePos + 20);
        this.returnCode = JobState.SUCCESS;
    }

    private void extractJSON(){

        //TODO: Parse, Export to ETLPacket, send to Matt


        int count = 0;
        int breakCount=0;
        int maxBreakCount=400;

        try{

//            JsonFactory factory = new JsonFactory();
//            JsonParser parser = factory.createParser(new File(this.filePath));
//
//            ObjectMapper mapper = new ObjectMapper();
//            List<Object[]> myObjects = mapper.readValue(parser, new TypeReference<List<Object>>(){});
//            System.out.println(myObjects);
//
//            parser.close();


            RandomAccessFile randomAccessFile = new RandomAccessFile(filePath,"r");

            StringBuffer stringBuffer = new StringBuffer();
            String line = randomAccessFile.readLine();

            while(count<12) {

                //If first line has '[', ignore and add to stringBuffer
                if (line.charAt(0) == '[') {

                    //Appends line without [
                    stringBuffer.append(line.substring(1));
                    line = line.substring(1);
                    String tempLine="";

                    if (stringBuffer.charAt(stringBuffer.length() - 1) == ',') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertToJSONObject(stringBuffer,tempLine);
                        stringBuffer.setLength(0);
                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == ']') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertToJSONObject(stringBuffer,tempLine);
                        stringBuffer.setLength(0);
                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == '}'){
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertToJSONObject(stringBuffer,tempLine);
                        stringBuffer.setLength(0);
                    }


                } else {
                    stringBuffer.append(randomAccessFile.readLine());
                    String tempLine="";

                    if (stringBuffer.charAt(stringBuffer.length() - 1) == ',') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertToJSONObject(stringBuffer,tempLine);
                        stringBuffer.setLength(0);
                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == ']') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertToJSONObject(stringBuffer,tempLine);
                        stringBuffer.setLength(0);
                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == '}'){
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertToJSONObject(stringBuffer,tempLine);
                        stringBuffer.setLength(0);
                    }

                }
                System.out.println("\n-------------------A Line---------------------");
                System.out.println("Line: " + stringBuffer);
                System.out.println("Byte location: " + randomAccessFile.getFilePointer());
                System.out.println("Count: "+count);

                count++;

                if(randomAccessFile.getFilePointer()>= randomAccessFile.length()){
                    System.out.println("END OF FILE - CLOSING");
                    randomAccessFile.close();
                    break;
                }

                inputETLPacket();
                readContent();
            }


//
//            while(breakCount<maxBreakCount) {
//                if(randomAccessFile.readLine().charAt(0)=='['){
//                    stringBuffer.append(randomAccessFile.readLine().substring(1));
//                    System.out.println(stringBuffer);
//                }
//
//                try {
//                    JSONObject jsonObject = new JSONObject(stringBuffer);
//                }catch(Exception e){
//                    System.out.println("Failed to create JSON Object...retrying");
//                }
//
//            }

        }catch(IOException e){
            e.printStackTrace();
            this.returnCode = JobState.FAILED;
        }
    }

    private void convertToJSONObject(StringBuffer stringBuffer, String tempLine){
        try {
            System.out.println("\n-----------------------Convert JSON with Bracket--------------------");
            System.out.println(tempLine);
            JSONObject convertedObject = new JSONObject(tempLine);
            System.out.println("Successful in converting");

            //TODO: Export to ETLPacket
            if(this.fieldName.isEmpty()){
                this.fieldName = new ArrayList<String>(convertedObject.keySet());

            }


        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Failed to convert");
        }
    }

    private void extractXML(){
        //TODO: Parse, Export to ETLPacket, send to Matt
        try{
            File xmlFile = new File(this.filePath);

            XmlMapper xmlMapper = new XmlMapper();
            List entries = xmlMapper.readValue(xmlFile,List.class);

            ObjectMapper jsonMapper = new ObjectMapper();
            String json = jsonMapper.writeValueAsString(entries); //Converted entire file to JSON String

        }catch(Exception e){

        }
    }

    private void inputETLPacket(){
        JSONObject header = new JSONObject();
        header.put("header",fieldName);

        JSONObject contents = new JSONObject();
        contents.put("contents",rows);

        etlPacket.put("data",header);
        etlPacket.put("data",contents);
    }

    private void sendByteData(){
        //TODO: Figure out how to send data back to DataSegmentationTask
    }

    private void readContent(){
        JSONObject etlPacket = parent.getETLPacketFromParent();
        System.out.print(etlPacket);
    }
}
