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

    //TODO: Put modified data back in the packet

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

        makeNewHeader(transforms);
        JSONArray headersToKeep = headerIndexesToKeep();
        //loop through each line
        for (int x =0; x<dataContents.length(); x++){
            JSONArray line = getLine(dataContents, x);
            System.out.println("Pre Transform: " + line);
            line = doFilters(line,filters);
            line = doTransformations(transforms,line);
            line = doMappings(line, headersToKeep);
            System.out.println("Current Line: " + line);
            dataContents.put(x, line);
            System.out.println("Headers to Keep"+headersToKeep);


           }
        return 1;
    }


    private JSONArray getLine(JSONArray content, int line_num) {
        return content.getJSONArray(line_num);
    }

    private JSONArray doMappings(JSONArray line, JSONArray toKeep) {
        JSONArray sendback = new JSONArray();
        for (int x =0; x<toKeep.length();x++){
            sendback.put(x, line.get(toKeep.getInt(x)));
        }

        return sendback;
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

    private JSONArray doFilters(JSONArray line,JSONObject filters) {
        for (int x=1; x<filters.length()+1;x++) {
            String curFilter = "filter" + x;
            String curFilterColumn = filters.getJSONObject(curFilter).get("source_column").toString();
            int indexOfColumn = getArrayIndex(sourceHeader,curFilterColumn);
            //String curFilterValue = filters.getJSONObject(curFilter).get("filter_value").toString();
            String curFilterSymbol = filters.getJSONObject(curFilter).get("equality_test").toString();
            //String curFilterNumber = curFilterValue.split(" ")[1];

            if (curFilterSymbol.equals("EQ")) {
                if (filters.getJSONObject(curFilter).get("filter_value") instanceof String) {
                    if (filters.getJSONObject(curFilter).get("filter_value").equals(line.get(indexOfColumn))) {
                        System.out.println("THE INFO YOU ARE LOOKING FOR " + line.get(indexOfColumn));
                    }
                }
                if (filters.getJSONObject(curFilter).get("filter_value") instanceof Double) {
                    System.out.println("INSIDEINSIDEINSIDEINSIDE");
                    if (filters.getJSONObject(curFilter).get("filter_value") == line.get(indexOfColumn)) {
                        System.out.println("THE INFO YOU ARE LOOKING FOR " + line.get(indexOfColumn));
                    }
                }
                if (filters.getJSONObject(curFilter).get("filter_value") instanceof Integer) {
                    if (filters.getJSONObject(curFilter).getInt("filter_value") == Integer.parseInt(line.get(indexOfColumn).toString())) {
                        System.out.println("THE INFO YOU ARE LOOKING FOR " + line.get(indexOfColumn));
                    }
                }
            }



            System.out.println("IN FILTER: " + curFilterColumn);

        }


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
    private void makeNewHeader(JSONObject transforms){
        for (int x=1; x<transforms.length()+1;x++) {
            String curTransform = "transform" + x;
            String newField = getCurrTransformNewField(transforms, curTransform);
            System.out.println("newField is: " + newField);
            if (newField.equals("")) {

            } else if (checkHeaderDuplicate(newHeader, newField)) {
                newHeader.put(newHeader.length(), newField);
            }

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

