
package org.idio.neo4j.graphson;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.idio.neo4j.graph.model.Node;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;

/*
 * Transforms a GraphSON representation of idiontology
 * into a CSV representation used by the CSV importer
 */
public class GraphsonToCSV {
    
    //Streaming Json file
    private Gson gson;
    // Path to the json file with the graph
    private String path_to_graphson_file;
    // List of accepted relationships
    private List<String> accepted_node_properties;
    // map from neo4j_ids to dump_ids
    private HashMap<Integer,Integer> map_neo4j_node_ids_to_csv_nodes_ids;
    // the next dump id 
    private int current_csv_node_id;
    
    public GraphsonToCSV(String path_to_graphson_file){
           gson = new Gson();
           this.path_to_graphson_file = path_to_graphson_file;
           current_csv_node_id = 1;
           map_neo4j_node_ids_to_csv_nodes_ids = new HashMap<Integer,Integer>();
    }
    
    /*
     * Given a value of a property removes the end of lines
     * and tabs and returns it
     * @param value Property Value
     */
    private String clean_node_property(String value){
            value = value.replace("\t","").replace("\n","").replace("\r","").replace(System.getProperty("line.separator"),"");
            value= value.replace("\\t","").replace("\\n", "").replace("\\r","").replace("\r\n","").replace("\\r\\n", "");
            value =value.trim();
            return value;
    }
    
    /*
     * Adds a neo4j_id to the mapping between neo4j_ids to batch_ids
     * @param node_neo4j_id a neo4j id of  node
     */
    private void add_neo4j_to_batchidmapping(int node_neo4j_id){
        map_neo4j_node_ids_to_csv_nodes_ids.put(node_neo4j_id, current_csv_node_id);
        current_csv_node_id++;
    }
    
    /*
     * Given a property type and value, it checks the value accordding to its type
     * Raising an error in case type and value can't be matched.
     * In case the value is null, it is replaced by the default value of its type
     * @param type property type i.e: string, int, double..
     * @param value the value of a property
     * @return value value of property assured to be not null
     */
    private String check_node_property_type(String type, String value){
        String new_value ="";
        if (value==null){
               if (!type.equals("int") && !type.equals("double") ){
                          new_value="__NULL__";
                }
                else{
                         new_value ="0";
                }
        }else{
            try{
                switch (type) {
                    case "int":
                           Integer.parseInt(value);
                           break;
                    case "double":
                           Double.parseDouble(value);
                           break;
                 }
               }catch(NumberFormatException e){
                   
                   System.err.println("CAST PROBLEM cant convert: "+value+" to  the specified type:"+type);
                   System.exit(1);
               }
             
             new_value= value;
          
        }
       return new_value;
    }
    
    /*
     * Returns the CSV representation of a node
     * @param node a hashmap with the properties and values of a node
     */
    private String get_node_csv_representation(Node node){
        String node_csv_representation = "";
        int current_id = Integer.parseInt((String) node.getSingle("_id"));
        for (String key:accepted_node_properties){
            
            // separate name of property from type
            String[] splitted_property = key.split(":");
            String property_name = splitted_property[0];
            String type = splitted_property[1];
            String value = "";

            value = node.getCSVRepresentation(property_name);

            // handle special types and null value
            value = check_node_property_type(type, value);
            
            value = clean_node_property(value);
            
            node_csv_representation  += value + "\t";
        }
        node_csv_representation = node_csv_representation.trim();
        
        int node_neo4j_id = Integer.parseInt(node.getSingle("_id"));
        this.add_neo4j_to_batchidmapping(node_neo4j_id);
     
        return node_csv_representation;
    }
    
    /*
     * Given a neo4j id, it returns its dump id.
     * @param neo4j_id 
     */
    private int get_nodeid_by_neo4jid(int neo4j_id){
        return this.map_neo4j_node_ids_to_csv_nodes_ids.get(neo4j_id);
    }
    
    /*
     * Given a relationship returns its csv representation
     * @param relationship a hashmap with the properties and values of a relationship
     */
    private String get_relationship_csv_representation(Properties relationship){
        int start_node = get_nodeid_by_neo4jid(Integer.parseInt((String)relationship.get("_outV")));
        int end_node =  get_nodeid_by_neo4jid(Integer.parseInt((String)relationship.get("_inV")));
        String relationship_csv_representation = start_node+"\t";
        relationship_csv_representation +=end_node+"\t";
        relationship_csv_representation +=relationship.get("_label");
        return relationship_csv_representation;
    }
    
     /*
     * returns the  CSV header for the nodes file
     */
    private String  get_node_csv_header(){
        String header_line = "";
        for (String accepted_property:accepted_node_properties){
            accepted_property= accepted_property.trim();
            accepted_property = accepted_property.replace("\n", "").replace("\r","");
            header_line +=accepted_property+"\t";
        }
        header_line = header_line.trim();
        return header_line;
    }
    
    /*
     * returns the CSV header for relationships file
     */
    private String get_relationship_csv_header(){
         String header_line = "start/tend/ttype";
         return header_line;
    }
    
     /*
     * Generates the nodes.csv and relationships.csv files
     */
    public void export_to_csv(String output_file_path, String[] accepted_node_properties) throws FileNotFoundException, UnsupportedEncodingException, IOException{
        this.accepted_node_properties= Arrays.asList(accepted_node_properties);
        System.out.println(this.accepted_node_properties);
        InputStream input_stream =  new FileInputStream(path_to_graphson_file);
        JsonReader reader = new JsonReader(new InputStreamReader(input_stream, "UTF-8"));
        
        String node_output_path = output_file_path+"nodes.csv";
        String relationship_output_path = output_file_path+"relationships.csv";
        
        //get nodes.csv and relationships.csv
        read_nodes(reader, node_output_path);
        read_relationships(reader, relationship_output_path);
    }


    private Node readNode(JsonReader reader)throws IOException{
        Map<String, ArrayList<String> > arrayProperties =  new HashMap<String, ArrayList<String>>();
        reader.beginObject();

        while(!reader.peek().equals(JsonToken.END_OBJECT)){
            String propertyName = reader.nextName();

            JsonToken nextToken = reader.peek();

            if (nextToken== JsonToken.BEGIN_ARRAY){
                reader.beginArray();
                ArrayList<String> list = new ArrayList<String>();
                while(reader.hasNext()){
                    String value =  reader.nextString();
                    list.add(value);
                }
                arrayProperties.put(propertyName, list);
                reader.endArray();
            }
            else if(nextToken == JsonToken.BOOLEAN){
                Boolean value =  reader.nextBoolean();
                ArrayList<String> values = new ArrayList<String>();
                values.add(value.toString());
                arrayProperties.put(propertyName, values);
            }
            else {
                String value =  reader.nextString();
                ArrayList<String> values = new ArrayList<String>();
                values.add(value);
                arrayProperties.put(propertyName, values);
            }
        }


        reader.endObject();
        return new Node(arrayProperties);
    }
    
     /*
     * Read nodes from the json reader 
     * and exports them to the given output path as csv lines.
     */
    private void read_nodes( JsonReader reader, String output_path) throws IOException{
        
        //reading the headers of the json file
        reader.beginObject();
        reader.nextName();
        reader.nextString();
        String name = reader.nextName();
        reader.beginArray();
        
        //opening the output file
        BufferedWriter output_file_buf_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_path),"UTF-8"));

        System.out.println("processing nodes..");
        
        // Get the CSV header and write it
        String csv_header = this.get_node_csv_header(); 
        output_file_buf_writer.write(csv_header+"\n");
 
        // Gets each node and gets its csv representation
        if (name.equals("vertices")){
            while (reader.hasNext()) {
                Node n = readNode(reader);
                String node_csv_representation =get_node_csv_representation(n);
                output_file_buf_writer.write(node_csv_representation+"\n");
                
            }
        }
        reader.endArray();
        output_file_buf_writer.close();
    }
    
      /*
     * Read Relationships from the json reader 
     * and exports them to the given output path as csv lines.
     */
    private void  read_relationships(JsonReader reader, String output_path) throws IOException{
        String name = reader.nextName();
        reader.beginArray();
         
         System.out.println("processing relationships..");
         
        // opening the output file
        BufferedWriter output_file_buf_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_path),"UTF-8"));
        
        //gets each relationship and gets its csv representation
        if (name.equals("edges")){
            while (reader.hasNext()) {  
                Properties relationship_data = gson.fromJson(reader, Properties.class);
                String relationship_csv_representation = this.get_relationship_csv_representation(relationship_data);
                output_file_buf_writer.write(relationship_csv_representation+"\n");
            }
        }
        reader.endArray();
        output_file_buf_writer.close();
    }
}
