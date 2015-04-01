package org.idio.neo4j.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.idio.neo4j.csv.tools.IndexCSVGenerator;
import org.idio.neo4j.csv.tools.RelationshipChecker;

import org.idio.neo4j.graphson.GraphsonToCSV;

public class App 
{
    
    public static void check_relationships(String args[]){
        try {
            String path_to_rel_csv_file = args[1];
            String path_to_output_file =args[2];
            RelationshipChecker checker = new RelationshipChecker(path_to_rel_csv_file);
            checker.get_csvfile_non_repeated_relationships(path_to_output_file);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void generate_index(String args[]){
            String input_csv_node_file = args[1];
            String output_csv_index_file =args[2];
            String indexed_properties = args[3];
            String[] list_of_indexed_properties = indexed_properties.split(",");
            
         try {
             IndexCSVGenerator csv_generator = new IndexCSVGenerator(input_csv_node_file, list_of_indexed_properties);
             csv_generator.get_csv_file(output_csv_index_file);
         } catch (FileNotFoundException ex) {
             Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
         } catch (IOException ex) {
             Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
         }
    }
    
    public static void json2csv(String args[]){
            String input_graphson_file = args[1];
            String output_folder =args[2];
            String accepted_properties = args[3];
            String[] list_of_accepted_properties = accepted_properties.split(",");
            
       try {
                    GraphsonToCSV graphson_exporter = new GraphsonToCSV(input_graphson_file);
                    graphson_exporter.export_to_csv(output_folder, list_of_accepted_properties);
                } catch (IOException ex) {
                    Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
             } 
    }
    
    public static void main( String[] args )
    {
        
        String command = args[0];
        switch (command) {
                    // Get Nodes, rels .csv
                    case "json2csv":
                            System.out.println("Exporting Graphson to CSV....");
                           json2csv(args);
                           break;
                    // Get Index.csv
                    case "generate-index":
                           System.out.println("Generating index file....");
                           generate_index(args);
                           break;
                     // Check rels
                    case "check-relationships":
                           System.out.println("Checking relationship uniqueness...");
                           check_relationships(args);
                           break;
          }  

    }
}
