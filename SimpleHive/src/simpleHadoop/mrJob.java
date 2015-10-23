/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHadoop;

import java.util.LinkedList;
import java.util.Set;

/**
 * An interface for implementing a map reduce job, to be run by the 'hadoop' driver.<br>
 * See wordCount.java for an example
 * @author toddbodnar
 */
public interface mrJob <mapInType,keyType,valueType>{
    
    /**
     * Run first, used to build tasks for the mapper
     * @param cont 
     */
    public void init(context cont);
    
    /**
     * Take an input and emit to to key/value pairs to the context
     * @param input
     * @param cont 
     */
    public void map(mapInType input, context cont);
    
    /**
     * Given a set of values for a key, do something with it
     * @param key
     * @param values 
     */
    public void reduce(keyType key, LinkedList<valueType> values);
}
