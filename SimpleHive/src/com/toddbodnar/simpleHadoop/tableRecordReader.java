/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHadoop;

import com.toddbodnar.simpleHive.IO.file;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author toddbodnar
 */
public class tableRecordReader extends RecordReader<IntWritable,Text>{

    table theTable;
    file theFile;
    int lineNumber = 0;
    String current = null,next;
    boolean nextLineEnd;
    public tableRecordReader(table t)
    {
        if(t==null)
        {
            nextLineEnd=true;
            return;
        }
        theTable = t;
        theFile = t.getFile();
        t.reset();
        next = theFile.readNextLine();
        nextLineEnd = false;
    }
    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        theFile.resetStream();
        ;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //a bit of weirdness going on here to get the first line
        if(nextLineEnd)
            return false;
        nextLineEnd = !theFile.hasNext();
        current=next;
        next = theFile.readNextLine();
        lineNumber++;
        return true;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return new IntWritable(lineNumber);
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
