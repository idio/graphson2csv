package org.idio.neo4j.csv.tools;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Iterator;
import org.idio.neo4j.graph.model.Relationship;

/**
 * Checks the relationships.csv file, getting rid of repeated elements.
 */
public class RelationshipChecker {
    
    // path to the relationship_csv_file
    private String relationship_csv_file;
    
    /*
     * @param path_to_rel_file path to the csv file with relationships
     */
    public RelationshipChecker(String path_to_rel_file){
        relationship_csv_file = path_to_rel_file;
    }

    /*
     * Parses a line from the relationships.csv file.
     * @param line relationship from the relationships.csv file
     * @return a relationship Object
     */
    private Relationship parse_line(String line){
        String[] splitted_line = line.split("\t");
        
        int start_node = Integer.parseInt(splitted_line[0]);
        int end_node = Integer.parseInt(splitted_line[1]);
        String type = splitted_line[2].replace("/n", "");
        
        return new Relationship(start_node,end_node,type);
    }
    
    /*
     * Reads the relationships from the csv file and output a new relationships.csv file only containing
     * unique relationships.
     * @param output_file path of the output file with the unique set of relationships
     */
    public void get_csvfile_non_repeated_relationships(String output_file) throws UnsupportedEncodingException, FileNotFoundException, IOException{        
        HashSet set = new HashSet<Relationship>();
        
        //loads file into memory
        BufferedReader bf  = new BufferedReader(new InputStreamReader(new FileInputStream(relationship_csv_file),"UTF-8"));
        BufferedWriter output_file_buf_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_file),"UTF-8"));
        
        System.out.println("reading file...");
        String line;
        while( (line=bf.readLine())!=null   ){
                   Relationship new_relationship=parse_line(line);
                   set.add(new_relationship);
        }
        bf.close();
        
        // Writting the outfile of relationships
        Iterator<Relationship>rel_iterator =  set.iterator();
        
        System.out.println("writting unique set of relationships ..");
        
        while(rel_iterator.hasNext()){
            Relationship r = rel_iterator.next();
            String new_line = r.export_to_csv()+"\n";
            output_file_buf_writer.write(new_line);
        }
        
        output_file_buf_writer.close();
    }
    
}
