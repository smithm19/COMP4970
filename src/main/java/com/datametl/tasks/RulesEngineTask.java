package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.List;
import java.util.ArrayList;

/**
 * Created by smithm19 on 2/6/17.
 */
public class RulesEngineTask implements Task {

    //TODO: Make newHeader == sourceHeader. Then add whatever isn't in newHeader from DestinationHeader to newHeader.
    //TODO: Now remove everything that isn't in destinationHeader from newHeader then sort it.

    private JobState current_state = JobState.RUNNING;
    private SubJob parent = null;
    private JSONArray newHeader;
    private JSONArray destinationHeader;
    private JSONArray sourceHeader;



    public void apply() {

        JSONObject newPacket = parent.getETLPacketFromParent();
        idiot(newPacket);

        current_state = JobState.SUCCESS;
    }


    public JobState getResult() {

        return current_state;

    }

    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    public SubJob getParent() {
        return this.parent;
    }

    private int idiot(JSONObject pckt) {
        JSONObject rules = pckt.getJSONObject("rules");
        JSONObject transforms = rules.getJSONObject("transformations");
        JSONObject mappings = rules.getJSONObject("mappings");
        JSONObject filters = rules.getJSONObject("filters");
        JSONObject packetData = pckt.getJSONObject("data");
        JSONArray dataContents = packetData.getJSONArray("contents");
        this.sourceHeader = packetData.getJSONArray("source_header");
        this.destinationHeader = packetData.getJSONArray("destination_header");
        this.newHeader = sourceHeader;

        preTransform(transforms);
        JSONArray headersToKeep = headerIndexesToKeep();
        //loop through lines
        for (int x =0; x<dataContents.length(); x++){
            JSONArray line = getLine(dataContents, x);
            //line = doMappings(line);
            System.out.println("Pre Transform: " + line);
            line = doTransformations(transforms,line);
            //line=editLine(line,headersToKeep);
            System.out.println("Current Line: " + line);
           //System.out.println("headersToKeep: " + headersToKeep);
            //line = doFilters(line);
            dataContents.put(x, line);


           }
        return 1;
    }


    private JSONArray getLine(JSONArray content, int line_num) {
        return content.getJSONArray(line_num);
    }

    private JSONArray doMappings(JSONArray line) {
        int line_length = line.length();
        List<Integer> del_list = new ArrayList<Integer>();
        for (int x = 0; x < line_length; x++) {
            if (line.get(x).equals("Matt")) {
                del_list.add(x);
            }
        }
        for (int y = 0; y < del_list.size(); y++) {
            line.remove(del_list.get(y));
        }


        return line;
    }

    private JSONArray doTransformations(JSONObject transforms,JSONArray line) {
        //loop through all transformations for a single line
        for (int x=1; x<transforms.length()+1;x++) {
            String curTransform = "transform" + x;
            String newField = getCurrTransformNewField(transforms, curTransform);
            int indexToGet;
            String value;
            String transformValue;
            String curSource;



            //can be MULT, DIV, POW, ADD, SUB
            if (getCurrTransformSymbol(transforms, curTransform).equals("MULT")) {
                if(newField.equals("")) {
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    line.put(indexToGet, Double.parseDouble(transformValue) * Double.parseDouble(value));

                }else{
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.newHeader,newField);
                    value = line.get(getArrayIndex(sourceHeader,curSource)).toString();

                    line.put(indexToGet,Double.parseDouble(transformValue) * Double.parseDouble(value));
                }
            }
            else if (getCurrTransformSymbol(transforms, curTransform).equals("DIV")) {
                if(newField.equals("")) {
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    line.put(indexToGet, Double.parseDouble(transformValue) / Double.parseDouble(value));
                }else{
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.newHeader,newField);
                    value = line.get(getArrayIndex(sourceHeader,curSource)).toString();

                    line.put(indexToGet,Double.parseDouble(transformValue) / Double.parseDouble(value));
                }
            }
            else if (getCurrTransformSymbol(transforms, curTransform).equals("ADD")) {
                if(newField.equals("")) {
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    line.put(indexToGet, Double.parseDouble(transformValue) + Double.parseDouble(value));
                }else{
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.newHeader,newField);
                    value = line.get(getArrayIndex(sourceHeader,curSource)).toString();

                    line.put(indexToGet,Double.parseDouble(transformValue) + Double.parseDouble(value));
                }
            }
            else if (getCurrTransformSymbol(transforms, curTransform).equals("SUB")) {
                if(newField.equals("")) {
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    line.put(indexToGet, Double.parseDouble(transformValue) - Double.parseDouble(value));
                }else{
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.newHeader,newField);
                    value = line.get(getArrayIndex(sourceHeader,curSource)).toString();

                    line.put(indexToGet,Double.parseDouble(transformValue) - Double.parseDouble(value));
                }
            }
            else if (getCurrTransformSymbol(transforms, curTransform).equals("POW")) {
                if(newField.equals("")) {
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    line.put(indexToGet, Math.pow(Double.parseDouble(transformValue), Double.parseDouble(value)));
                }else{
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.newHeader,newField);
                    value = line.get(getArrayIndex(sourceHeader,curSource)).toString();

                    line.put(indexToGet,Double.parseDouble(transformValue) - Double.parseDouble(value));
                    line.put(indexToGet, Math.pow(Double.parseDouble(transformValue),Double.parseDouble(value)));
                }

            }
        }



        return line;
    }

    private JSONArray doFilters(JSONArray line) {

        return line;
    }

    private int getArrayIndex(JSONArray arr, String toCompare) {
        for (int i = 0; i < arr.length(); i++) {
            if (toCompare.equals(arr.get(i))) {
                return i;
            }
        }
        return -1;
    }

    //code looks like garbage with this stuff everywhere, so I made Transformation functions to limit the grossness
    ////
    ////
    private String getCurrTransformNewField(JSONObject trans,String curTransform){

        if (trans.getJSONObject(curTransform).get("new_field").equals(null)){
            return "";
        }
        return trans.getJSONObject(curTransform).getString("new_field");
    }

    private String getCurrTransformSource(JSONObject trans, String curTransform){
        return trans.getJSONObject(curTransform).getString("source_column");
    }

    private String getCurrTransformValue(JSONObject trans, String curTransform){
        return trans.getJSONObject(curTransform).getString("transform").split(" ")[1];
    }
    private String getCurrTransformSymbol(JSONObject trans,String curTransform){
        return trans.getJSONObject(curTransform).getString("transform").split(" ")[0];
    }
    //Makes newHeader
    private void preTransform(JSONObject transforms){
        for (int x=1; x<transforms.length()+1;x++) {
            String curTransform = "transform" + x;
            String newField = getCurrTransformNewField(transforms, curTransform);
            System.out.println("newField is: " + newField);
            if (newField.equals("")) {

            } else if (checkHeaderDuplicate(newHeader, newField)) {
                newHeader.put(newHeader.length(), newField);
            }
            System.out.println("newHeader: " + newHeader);

        }
    }
    ////
    ////


    //returns an array of indexes to keep that match the newHeader to DestinationHeader
    private JSONArray headerIndexesToKeep(){
       JSONArray elementsToKeep = new JSONArray();

       for (int x=0; x<destinationHeader.length();x++){
           for(int y=0; y<newHeader.length();y++){
               if (newHeader.get(y).equals(destinationHeader.get(x))){
                   elementsToKeep.put(elementsToKeep.length(),getArrayIndex(newHeader,newHeader.get(y).toString()));
               }
           }
       }
       return elementsToKeep;
    }

    //A way to remove unwanted parts of line by passing it an array of indexes to keep (index array has to be fixed and so does this func)
    private JSONArray editLine(JSONArray line, JSONArray toKeep){
        JSONArray sendback = new JSONArray();
        for (int x =0; x<toKeep.length();x++){
            sendback.put(x, line.get(toKeep.getInt(x)));
        }
        return sendback;
    }


    private boolean checkHeaderDuplicate(JSONArray heads, String word){
        for (int x=0; x<heads.length(); x++){
            if (heads.get(x).equals(word)){
                return false;
            }
        }
        return true;
    }
}

