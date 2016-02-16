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
public abstract class mrJob <mapInType,keyType,valueType> implements java.io.Serializable{
    
    /**
     * Run first, used to build tasks for the mapper. By default, emit each line to the mappers
     * @param cont 
     */
    public void init(simpleContext cont)
    {
        table input = getInput();
        input.first();
        while(input.hasNextRow())
        {
            cont.add(input.get());
            input.nextRow();
        }
    }
    
    /**
     * Take an input and emit to to key/value pairs to the simpleContext
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
