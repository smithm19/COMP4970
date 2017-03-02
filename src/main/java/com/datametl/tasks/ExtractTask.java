package com.datametl.tasks;

import com.datametl.exception.JSONParsingException;
import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.channels.Channels;
import java.util.*;

/**
 * Created by TseAndy on 2/6/17.
 */
public class ExtractTask implements Task{

    private JobState returnCode = JobState.NOT_STARTED;
    private SubJob parent = null;
    private JSONObject etlPacket;

    private ArrayList<Object> fieldName = new ArrayList<Object>();
    private List<List<Object>> rows = new ArrayList<List<Object>>();

    private String filePath;
    private String fileType;
    private int docToRead;
    private long linesRead;
    private long bytePos;
    private long lastBytePos;

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
            try {
                extractJSON();
            }catch(JSONParsingException e){

            }
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
        etlPacket.put("current_byte_position", bytePos + 20);*/

        /*
        Task extractTask = new ExtractTask();
        SubJob newExtractJob = new SubJob(extractTask);
        boolean status = parent.getParent().addSubJob(newExtractJob);
        */

        returnCode = JobState.SUCCESS;

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


    //TODO: Parse with delimiters
    //TODO: Get Bytes to be read
    private void extractCSV() {

        int count=0;
        BufferedReader buff;
        RandomAccessFile raf = null;

        try{

            raf = new RandomAccessFile(new File(this.filePath),"r");

            raf.seek(bytePos);

            InputStream is = Channels.newInputStream(raf.getChannel());
            InputStreamReader isr = new InputStreamReader(is);
            buff = new BufferedReader(isr);
            CSVReader reader = new CSVReader(buff);
            Object[] nextLine;

            //Each nextLine is a row in CSV
            while((nextLine=reader.readNext())!=null&&count<this.docToRead){
                if(nextLine!=null) {
                    List<Object> listLine = Arrays.asList(nextLine);
                    if (this.fieldName.isEmpty()) {
                        //First line must be field names
                        this.fieldName = new ArrayList<Object>(listLine);
                    } else {

                        this.rows.add(listLine);
                        //System.out.println(listLine);
                    }
                }
                count++;
                //System.out.println(count);

            }

            //System.out.println("LINES READ: "+reader.getLinesRead());
            lastBytePos = raf.getFilePointer();

            reader.close();
            inputETLPacket(); //Puts information to ETLPacket
            this.etlPacket.put("current_byte_position", (lastBytePos-bytePos) + bytePos);

            readContent();
        }catch(IOException e){
            e.printStackTrace();
            this.returnCode = JobState.FAILED;
        }

        //this.etlPacket.put("current_byte_position", (lastBytePos-bytePos)+bytePos);


    }

    private void extractJSON() throws JSONParsingException {

        //TODO: Send to Matt


        int count = 0;
        int breakCount=0;
        int maxBreakCount=500;
        int docsRead=0;

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
            String line;
            if(bytePos==0){
                line = randomAccessFile.readLine();
            }else{
                randomAccessFile.seek(bytePos);
                line = randomAccessFile.readLine();
            }
            //String line = randomAccessFile.readLine();

            while(breakCount<maxBreakCount) {
                boolean convertSuccess=false;

                if(randomAccessFile.getFilePointer()>= randomAccessFile.length()){
                    System.out.println("END OF FILE - CLOSING");
                    randomAccessFile.close();
                    break;
                }

                //If first line has '[', ignore and add to stringBuffer
                if (line.charAt(0) == '[') {

                    //Appends line without [
                    stringBuffer.append(line.substring(1));
                    line = line.substring(1);
                    String tempLine="";

                    if (stringBuffer.charAt(stringBuffer.length() - 1) == ',') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                        stringBuffer.setLength(0);
                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == ']') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                        stringBuffer.setLength(0);
                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == '}'){
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                        stringBuffer.setLength(0);
                    }


                } else {
                    //If not the first line, append
                    stringBuffer.append(randomAccessFile.readLine());
                    String tempLine="";

                    if (stringBuffer.charAt(stringBuffer.length() - 1) == ',') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                        stringBuffer.setLength(0);
                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == ']') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                        stringBuffer.setLength(0);
                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == '}'){
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                        stringBuffer.setLength(0);
                    }

                }
//                System.out.println("\n-------------------A Line---------------------");
//                System.out.println("Line: " + stringBuffer);
//                System.out.println("Byte location: " + randomAccessFile.getFilePointer());
//                System.out.println("Count: "+count);

                //Increment counter to determine if valid
                breakCount++;


                if(convertSuccess){
                    docsRead++;
                    if(docsRead>=docToRead){
                        lastBytePos = randomAccessFile.getFilePointer();
                        break;
                    }
                }

                lastBytePos = randomAccessFile.getFilePointer();
            }

            if(breakCount>=maxBreakCount){
                this.returnCode = JobState.FAILED;
                throw new JSONParsingException("Parsing JSONObject timed out. File is either invalid, formatted incorrectly or the object is too large.");
            }else{
                inputETLPacket();
            }

            this.etlPacket.put("current_byte_position", (lastBytePos - bytePos)+bytePos);
            readContent();

        }catch(IOException e){
            e.printStackTrace();
            this.returnCode = JobState.FAILED;
        }
    }

    private boolean convertToJSONObject(String tempLine){
        try {
            //System.out.println("\n-----------------------Convert JSON with Bracket--------------------");
            //System.out.println(tempLine);
            JSONObject convertedObject = new JSONObject(tempLine);
            //System.out.println("Successful in converting");

            //Insert into fieldNames
            if(this.fieldName.isEmpty()){
                this.fieldName = new ArrayList<Object>(convertedObject.keySet());
            }

            //Mapping values to keys
            ArrayList<Object> content = new ArrayList<Object>();
            for(Object element : this.fieldName){
                //System.out.println("VALUE: "+convertedObject.get((String)element));
                content.add(convertedObject.get((String)element));
            }
            this.rows.add(content);


            //System.out.println("Successful in insertion");
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            //System.out.println("Failed to convert/insert");
            return false;
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

        JSONObject data = new JSONObject();
        JSONArray contents = this.etlPacket.getJSONObject("data").getJSONArray("contents");

        for(List<Object> listString : rows){
            contents.put(listString);
        }
        data.put("contents",contents);
        data.put("header",fieldName);
        etlPacket.put("data",data);
    }

    private void readContent(){
        JSONObject etlPacket = parent.getETLPacketFromParent();
        System.out.println(etlPacket+"\n");
    }
}
