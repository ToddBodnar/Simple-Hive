/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.toddbodnar.simpleHive.IO.file;
import com.toddbodnar.simpleHive.IO.ramFile;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHadoop.MapReduceJob;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author toddbodnar
 */
public class where extends query<Text,Text>{

    booleanTest theQuery;
    String partialQuery;
    /**
     * given a partial query (the part after the where) of the form:
     * _col1 < "string" AND _col2 = "string" AND _col3 = "string"
     * @param partialQuery 
     */
    public where(String partialQuery)
    {
        this.partialQuery = partialQuery; 
        
        result = new ramFile();
    }
    private file result;
    

    public void parseInput() {
        
        //replace the variables in the query string with the actual column numbers
        String split[] = partialQuery.split(" ");
        for(int ct=0;ct<split.length;ct++)
        {
            int colNum = getInput().getColNum(split[ct]);
            if(colNum!=-1)
                split[ct] = "_col"+colNum;
        }
        partialQuery = split[0];
        for(int ct=1;ct<split.length;ct++)
            partialQuery+=" "+split[ct];
        
        theQuery = new booleanTest(partialQuery);
        
    }





    @Override
    public Reducer getReducer() {
        return new Reducer()
                {
                    //just default behavior, since all processing is done map side
                };
    }
    

 
    @Override
    public Mapper<IntWritable[], Text, Text, Text> getMapper() {
        return new Mapper<IntWritable[], Text, Text, Text>()
                {
                    
                public void map(IntWritable key[], Text line, Mapper.Context cont) throws IOException, InterruptedException
                {
                        try {
                            if(theQuery.evaluate((Object[])line.toString().split(getInput().getSeperator())))
                            {
                                cont.write(line,null);
                            }           } catch (Exception ex) {
                            Logger.getLogger(where.class.getName()).log(Level.SEVERE, null, ex);
                        }
                }
                };}

 

    @Override
    public Class getKeyType() {
        return Text.class;
    }

    @Override
    public Class getValueType() {
        return Text.class;
    }
   
    public void setInput(table in)
    {
        super.setInput(in);
        parseInput();
    }
    
        public table getOutput()
    {
        if(super.getOutput()!=null)
            return super.getOutput();
        table result = new table(new ramFile(),getInput().getColNames());
        result.setSeperator(getInput().getSeperator());
        super.setOutput(result);
        return result;
    }
        
}
