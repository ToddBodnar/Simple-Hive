/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHadoop;

import com.toddbodnar.simpleHive.helpers.pair;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * A set of helpers for the mrJob and the driver
 * @author toddbodnar
 */
public class simpleContext<mapInKey, mapInValue, key, value,reduceOutKey,reduceOutValue>{
    HashMap<key,LinkedList<value>> data;
    LinkedList<pair> toProcess;
    LinkedList<pair<reduceOutKey,reduceOutValue>> results;
    HashMap<String,String> config;
    MapContext mapperContext;
    ReduceContext reducerContext;
    simpleContext(RecordReader recordReader) throws InterruptedException, IOException
    {
        data = new HashMap<>();
        toProcess = new LinkedList<>();
        results = new LinkedList<>();
        
        config = new HashMap<>();
        mapperContext = new mapContext(recordReader);
        reducerContext = new reduceContext();
    }
    
    public MapContext<mapInKey,mapInValue,key,value> getMapContext()
    {
        return mapperContext;
    }
    
    public ReduceContext<key,value,reduceOutKey,reduceOutValue> getReduceContext()
    {
        return reducerContext;
    }
    
    private class mapContext extends MapContext<mapInKey, mapInValue, key, value> {

        public mapContext(RecordReader records) {
            super(null, null, records, null, null, null, null);
        }
        

        @Override
        public void write(key k, value v) {
            LinkedList<value> set;
            if (data.containsKey(k)) {
                set = data.get(k);
            } else {
                set = new LinkedList<value>();
                data.put(k, set);
            }
            set.add(v);
        }
        
        

    }
    
    private class reduceContext extends ReduceContext<key, value,reduceOutKey,reduceOutValue> {

        public reduceContext() throws IOException, InterruptedException {
            super(null, null, null, null,null,null,null,null,null,null,null);
        }

        @Override
        public void write(reduceOutKey k, reduceOutValue v) {
            
            results.add(new pair(k,v));
        }

    }
    /**
     * Set something in the context's config
     * @param key
     * @param value 
     */
    public void set_arg(String key, String value)
    {
        config.put(key, value);
    }
    
    /**
     * Get a config's value, or null if not set
     * @param key
     * @return 
     */
    public String get_arg(String key)
    {
        return config.get(key);
    }
    
    /**
     * Adds a job for a mapper to process
     * @param job 
     */
    public void add_input_format(mapInKey key, mapInValue value)
    {
        toProcess.add(new pair<mapInKey,mapInValue>(key,value));
    }
    
    public LinkedList getResults()
    {
        return results;
    }
   
}
