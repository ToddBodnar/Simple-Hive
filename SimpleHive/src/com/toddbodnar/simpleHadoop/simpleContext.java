/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHadoop;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * A set of helpers for the mrJob and the driver
 * @author toddbodnar
 */
public class simpleContext{
    HashMap<Object,LinkedList<Object>> data;
    LinkedList<Object> toProcess;
    HashMap<String,String> config;
    simpleContext()
    {
        data = new HashMap<Object,LinkedList<Object>>();
        toProcess = new LinkedList<Object>();
        config = new HashMap<String,String>();
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
    public void add_input_format(Object job)
    {
        toProcess.add(job);
    }
    
    /**
     * Takes the output of a mapper
     * @param key
     * @param value 
     */
    public void emit_map(Object key, Object value)
    {
        LinkedList<Object> set;
        if(data.containsKey(key))
            set = data.get(key);
        else
        {
            set = new LinkedList<Object>();
            data.put(key, set);
        }
        set.add(value);
    }
}
