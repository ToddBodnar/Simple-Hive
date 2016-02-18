/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import java.util.LinkedList;
import com.toddbodnar.simpleHive.IO.file;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHadoop.MapReduceJob;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 *
 * @author toddbodnar
 */
public abstract class query<key,value> extends MapReduceJob<LongWritable,Text,key,value,Object,Text>{
    
    /**
     * By default, just process the table line-by-line (row-by-row)
     * @return 
     */
    public RecordReader<LongWritable,Text> getRecordReader()
    {
        return new LineRecordReader();
    }

    public void setInput(table in)
    {
        input = in;
    }
    
    
    public void setOutput(table out)
    {
        output = out;
    }
    
    
    public table getInput()
    {
        return input;
    }

    
    public table getOutput()
    {
        return output;
    }
    
    private table input,output;
}
