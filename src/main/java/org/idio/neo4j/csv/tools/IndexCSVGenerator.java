package org.idio.neo4j.csv.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Outputs a nodes_index.csv out of a nodes.csv file.
 */
public class IndexCSVGenerator {
    
    String path_to_csv_file;
    String[] list_of_indexed_properties;
    String[] property_names;
    
    /*
     * @param path_to_csv_file path to nodes.csv file
     * @param list_of_indexed_properties list of indexed_properties
     */
    public IndexCSVGenerator(String path_to_csv_file, String[] list_of_indexed_properties){
        this.path_to_csv_file = path_to_csv_file;
        this.list_of_indexed_properties = list_of_indexed_properties;
    }
    
    /*
     * Given the properties of a node it outputs the csv line to add to the index.csv
     * @param node properties of a node
     * @return string describing the indexing of a node in the csv format
     */
    private String get_csv_index_of_node(Properties node){
        String csv_node_index_entry =node.getProperty("_id");
        for(String property:list_of_indexed_properties){
            String[] splitted_indexed_property =property.split(":");
            String property_name=splitted_indexed_property[0];
            String property_value = (String)node.get(property_name);
            property_value=property_value.replace("\n","").replace("\t","");
            csv_node_index_entry+="\t"+property_value;
        }
        return csv_node_index_entry;
    }
    
    /*
     * Returns a csv file with the header of the nodes_index.csv file
     */
    private String get_csv_header(){
        String csv_file_header = "id";
        
        for(String property_name:this.list_of_indexed_properties){
            csv_file_header+="\t"+property_name;
        }
        
        return csv_file_header;
    }
    
    /*
     * Using the given nodes.csv file in the constructor, 
     * it will create a nodes_index.csv file in the given input path
     * @param output_file_path file path of the output nodes_index.csv file
     */
    public void get_csv_file(String output_file_path) throws FileNotFoundException, IOException{
       
        // Read the nodes.csv file
        FileInputStream reader = new FileInputStream(path_to_csv_file);
        BufferedReader buffered_line_reader = new BufferedReader(new InputStreamReader(reader));
        
        //Output File
        FileWriter writer = new FileWriter(output_file_path);
        String csv_file_header = get_csv_header();
        writer.write(csv_file_header+"\n");
        
        // Get the header of the csv file stating the properties
        String header_line_of_nodes_csv = buffered_line_reader.readLine();
        property_names  = header_line_of_nodes_csv.split("\t");
        
        String line;
        int line_number=1;
        
        while((line = buffered_line_reader.readLine())!=null){
                String[] splitted_line =  line.split("\t");
                Properties node_data = parse_line(splitted_line);
                // get the node properties
                Integer id= (Integer)line_number;
                node_data.put("_id", id.toString());
                
                // get the index representation for that node
                String csv_index_node_line = get_csv_index_of_node(node_data);
               
                writer.write(csv_index_node_line+"\n");
                line_number++;
        }
        writer.close();
        
    }
    
    /*
     * Parses one line from the nodes.csv file, converting each field into a property
     */
    public Properties parse_line(String[] splitted_line){
        Properties line_properties =new Properties();
        
        for(int i = 0;i<this.property_names.length;i++){
            String property_name_without_type = property_names[i].split(":")[0];
            line_properties.put(property_name_without_type, splitted_line[i]);
        }
        
        return line_properties;
    }  
}
