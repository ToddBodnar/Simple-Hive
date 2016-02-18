/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHadoop;

import com.toddbodnar.simpleHive.IO.file;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author toddbodnar
 */
public class tableRecordReader extends RecordReader{

    table theTable;
    file theFile;
    int lineNumber = 0;
    String current = null;
    public tableRecordReader(table t)
    {
        theTable = t;
        theFile = t.getFile();
    }
    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        theFile.resetStream();
        ;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!theFile.hasNext())
            return false;
        current = theFile.readNextLine();
        lineNumber++;
        return true;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return lineNumber;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return current;
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
