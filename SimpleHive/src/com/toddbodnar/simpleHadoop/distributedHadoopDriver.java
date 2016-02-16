/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHadoop;

import com.toddbodnar.simpleHive.helpers.serialString;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.toddbodnar.simpleHive.IO.hdfsFile;
import com.toddbodnar.simpleHive.metastore.table;

/**
 * A driver to run mrJobs.
 * @author toddbodnar
 */
public class distributedHadoopDriver {

    /**
     * Runs a job
     * @param theJob the MapReduceJob to be run
     * @param verbose if true, output progress information
     */
    public static void run(MapReduceJob theJob, boolean verbose) throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, theJob.toString());
        job.setJarByClass(distributedHadoopDriver.class);
        
        job.setMapperClass(mrJobHadoopWrapper.map.class);
        job.setReducerClass(mrJobHadoopWrapper.reduce.class);
        
        
        conf.set("theMRJob", serialString.toString(theJob));
        
        //FileSystem fs = FileSystem.get(conf);
        
        FileInputFormat.setInputPaths(job, new Path(theJob.getInput().getFile().getLocation()));
        
        Path out = new Path("TMP_TABLE_"+theJob.hashCode());
        FileOutputFormat.setOutputPath(job,out);
        
        job.waitForCompletion(verbose);
        theJob.setOutput(new table(new hdfsFile(out), theJob.getOutput().getColNames()));
        
    }
}