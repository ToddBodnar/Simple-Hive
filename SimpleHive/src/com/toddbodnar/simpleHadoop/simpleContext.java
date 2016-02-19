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
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

/**
 * A set of helpers for the mrJob and the driver
 *
 * @author toddbodnar
 */
public class simpleContext<mapInKey, mapInValue, key, value, reduceOutKey, reduceOutValue> {

    HashMap<key, LinkedList<value>> data;
    LinkedList<pair> toProcess;
    LinkedList<pair<reduceOutKey, reduceOutValue>> results;
    map.Context mapperContext;
    ReduceContext reducerContext;
    Configuration theConfiguration;

    simpleContext(RecordReader recordReader, Class keyClass, Class valueClass) throws InterruptedException, IOException {
        toProcess = new LinkedList<>();
        data = new HashMap<>();
        results = new LinkedList<>();

        theConfiguration = new Configuration();
        mapperContext = new map().get(recordReader, theConfiguration);
        reducerContext = new reduce().get(theConfiguration,data,keyClass,valueClass);

    }

    public MapContext<mapInKey, mapInValue, key, value> getMapContext() {
        return mapperContext;
    }

    public ReduceContext<key, value, reduceOutKey, reduceOutValue> getReduceContext() {
        return reducerContext;
    }

    private class map extends Mapper<mapInKey, mapInValue, key, value> {

        public map.Context get(RecordReader records, Configuration theConfig) throws IOException, InterruptedException {
            return new Context(records, theConfig);
        }

        private class Context extends Mapper<mapInKey, mapInValue, key, value>.Context {//<mapInKey, mapInValue, key, value> {

            public Context(RecordReader records, Configuration theConfig) throws IOException, InterruptedException {
                super(theConfig, new TaskAttemptID(), records, new NullRecordWriter(), null, null, null);

            }

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
    }

    private static class NullRecordWriter extends RecordWriter {

        public void write(Object k, Object v) throws IOException, InterruptedException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        public void close(TaskAttemptContext tac) throws IOException, InterruptedException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }

    private class reduce extends Reducer<key, value, reduceOutKey, reduceOutValue> {

        public Reducer.Context get(Configuration theConfig,HashMap<key, LinkedList<value>> results, Class keyClass, Class valueClass) throws IOException, InterruptedException {
            return new reduceContext(theConfig,results,keyClass,valueClass);
        }

        private class reduceContext extends Reducer<key, value, reduceOutKey, reduceOutValue>.Context {

            HashMap<key, LinkedList<value>> data;
            Iterator<key> itr = null;
            key next = null;
            public reduceContext(Configuration theConfig,HashMap<key, LinkedList<value>> results, Class keyClass, Class valueClass) throws IOException, InterruptedException {
                super(theConfig, new TaskAttemptID(), new RawKeyValueIterator() {

                    @Override
                    public DataInputBuffer getKey() throws IOException {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public DataInputBuffer getValue() throws IOException {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public boolean next() throws IOException {
                        return true;                    }

                    @Override
                    public void close() throws IOException {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public Progress getProgress() {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }
                }, null, null, new NullRecordWriter(), null, null, null, keyClass,valueClass);
                data = results;
            }

            @Override
            public void write(reduceOutKey k, reduceOutValue v) {

                results.add(new pair(k, v));
            }

            public boolean nextKey()
            {
                if(itr==null)
                    itr = data.keySet().iterator();
                
                if(itr.hasNext())
                {
                    next = itr.next();
                    return true;
                }
                return false;
            }
            
            public key getCurrentKey()
            {
                return next;
            }
            
            public Iterable<value> getValues()
            {
                return data.get(next);
            }
        }
    }

    public Configuration getConfiguration() {
        return theConfiguration;
    }

    /**
     * Adds a job for a mapper to process
     * @param key
     * @param value
    */
    public void add_input_format(mapInKey key, mapInValue value) {
        toProcess.add(new pair<>(key, value));
    }

    public LinkedList getResults() {
        return results;
    }

}
