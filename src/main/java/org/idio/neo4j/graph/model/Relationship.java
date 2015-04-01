package org.idio.neo4j.graph.model;

public class Relationship{
    
   public int neo4j_start_node_id;
   public int neo4j_end_node_id;
   public String type;
    
    public Relationship(int neo4j_start_node_id, int neo4j_end_node_id,String type){
        this.neo4j_start_node_id = neo4j_start_node_id;
        this.neo4j_end_node_id = neo4j_end_node_id;
        this.type = type;
    }
    
    public boolean equals(Object o){
        Relationship another = (Relationship) o;
        
        if (this.neo4j_start_node_id == another.neo4j_start_node_id && this.neo4j_end_node_id==another.neo4j_end_node_id && this.type.equals(another.type)){
          return true;
        }
        return false;
    }
    
     public int hashCode()  
        {    
        String signature = neo4j_start_node_id+"_"+neo4j_end_node_id+"_"+type;
        return signature.hashCode();
    }
     
     public String export_to_csv(){
         return neo4j_start_node_id+"\t"+neo4j_end_node_id+"\t"+type;
     }
}
