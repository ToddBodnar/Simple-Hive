/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHadoop;

import com.toddbodnar.simpleHive.IO.file;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author toddbodnar
 */
public class tableRecordReader extends RecordReader<Object,Text>{

    LinkedList<table> theTables;
    
    String current = null,next;
    Iterator<table> currentTable;
    int currentTableItr = 0;
    int lineNumber = 0;
    file currentFile = null;
    boolean nextLineEnd;
    public tableRecordReader(table t)
    {
        if(t==null)
        {
            nextLineEnd=true;
            return;
        }
        theTables = new LinkedList<>();
        theTables.add(t);
        
    }
    
    public void addMoreTables(table t)
    {
        theTables.add(t);
    }
    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        currentTable = theTables.iterator();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(currentFile == null)
        {
            if(!currentTable.hasNext())
                return false;
            currentFile = currentTable.next().getFile();
            currentFile.resetStream();
            currentTableItr++;
            lineNumber = 0;
        }
        
        if(!currentFile.hasNext())
        {
            currentFile = null;
            return nextKeyValue();
        }
        
        current = currentFile.readNextLine();
        lineNumber++;
        return true;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return new IntWritable[]{new IntWritable(currentTableItr),new IntWritable(lineNumber)};
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text(current);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        ;
    }
    
}
