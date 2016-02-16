/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHadoop;

import java.util.LinkedList;
import java.util.Set;
import com.toddbodnar.simpleHive.IO.ramFile;
import com.toddbodnar.simpleHive.metastore.table;

/**
 * An interface for implementing a map reduce job, to be run by the 'hadoop' driver.<br>
 * See wordCount.java for an example
 * @author toddbodnar
 */
public abstract class MapReduceJob <mapInType,keyType,valueType> implements java.io.Serializable{
    
    /**
     * Run first, used to build tasks for the mapper. By default, emit_map each line to the mappers
     * @param cont 
     */
    public void inputFormat(simpleContext cont)
    {
        table input = getInput();
        input.first();
        while(input.hasNextRow())
        {
            cont.add_input_format(input.get());
            input.nextRow();
        }
    }
    
    /**
     * Called before any mapers are processed
     * @param cont 
     */
    public void init_map(simpleContext cont){;}
    
    /**
     * Called after any mappers are run
     * @param cont 
     */
    public void end_map(simpleContext cont){;}
    
    /**
     * Called before any reducers are processed
     * @param cont 
     */
    public void init_reduce(simpleContext cont){;}
    
    /**
     * Called after any reducers are run
     * @param cont 
     */
    public void end_reduce(simpleContext cont){;}
    
    /**
     * Take an input and emit_map to to key/value pairs to the simpleContext
     * @param input
     * @param cont 
     */
    public abstract void map(mapInType input, simpleContext cont);
    
    /**
     * Given a set of values for a key, do something with it
     * @param key
     * @param values 
     */
    public abstract void reduce(keyType key, LinkedList<valueType> values);
    
    /**
     * Sets the input for a job
     * @param in 
     */
    public abstract void setInput(table in);
    
    public abstract void setOutput(table table);
    
    /**
     * Gets the main input for a job
     * @return 
     */
    public abstract table getInput();

    public abstract table getOutput();

    
}
