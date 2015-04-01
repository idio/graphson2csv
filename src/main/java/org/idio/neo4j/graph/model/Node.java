package org.idio.neo4j.graph.model;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by dav009 on 01/04/2015.
 */
public class Node{


    private Map<String, ArrayList<String>> arrayProperties;

    public Node( Map<String, ArrayList<String>> arrayProperties){
         this.arrayProperties =  arrayProperties;
    }

    public String getSingle(String key){
        return arrayProperties.get(key).get(0);
    }

    public String toString(){
        String output = "";
         for (String key:arrayProperties.keySet()){
             output = output + "\n key : " + getCSVRepresentation(key);
         }
        return output;
    }

    public String getCSVRepresentation(String key){
        ArrayList<String> values =  arrayProperties.get(key);

        StringBuilder csvValue = new StringBuilder();

        if(values==null)
            return null;

        for (String value : values) {
            csvValue.append(value + "|");
        }
        return new String(csvValue.deleteCharAt(csvValue.length() - 1));


    }

}
