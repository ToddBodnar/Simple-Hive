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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * An interface for implementing a map reduce job, to be run by the 'hadoop' driver.<br>
 * See wordCount.java for an example
 * @author toddbodnar
 */
public abstract class MapReduceJob <mapInKey,mapInValue,key,value,reduceOutKey,reduceOutValue>{
    
    /**
     * Build your mapreduce.RecordReader here, converts files (or similar) into key,value pairs for the mapper
     */
    public abstract RecordReader<mapInKey,mapInValue> getRecordReader();
    
    /**
     * Build your mapreduce.Mapper here, takes in records of form <mapInKey,mapInValue> and maps them to something of the form <key,value>
     */
    public abstract Mapper<mapInKey,mapInValue,key,value> getMapper();
    
    /**
     * Build your mapreduce.Reducer here, takes in set of <value> based on a key <key> outputs to files of the format <reduceOutKey,reduceOutValue>
     */
    public abstract Reducer<key,value,reduceOutKey,reduceOutValue> getReducer();
    
    /**
     * Sets a "table" to be the input
     * @param in 
     */
    public abstract void setInput(table in);
    
    /**
     * Sets a table to be written to
     * @param out 
     */
    public abstract void setOutput(table out);
    
    /**
     * Gets the main input for a job
     * @return 
     */
    public abstract table getInput();

    /**
     * Gets the main output for a job
     * @return 
     */
    public abstract table getOutput();

    public abstract Class getKeyType();
    
    public abstract Class getValueType();
    
    /**
     * Writes the configuration for the MapReduceJob, which will then be shunted to each of the task nodes
     * @param conf 
     */
    public abstract void writeConfig(Configuration conf);
    
}
