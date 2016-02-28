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
import com.toddbodnar.simpleHive.helpers.GetConfiguration;
import com.toddbodnar.simpleHive.helpers.settings;
import com.toddbodnar.simpleHive.metastore.table;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
        Configuration conf = GetConfiguration.get();
        Job job = Job.getInstance(conf, theJob.toString());
        job.setJarByClass(distributedHadoopDriver.class);
        
        job.setMapperClass(theJob.getMapper().getClass());
        job.setReducerClass(theJob.getReducer().getClass());
        
        job.setMapOutputKeyClass(theJob.getKeyType());
        job.setMapOutputValueClass(theJob.getValueType());
        
        theJob.writeConfig(job.getConfiguration());
        
        MultipleInputs.addInputPath(job,hdfsFile.transferToHDFS(theJob.getInput().getFile()).getPath(), TextInputFormat.class);
       
        System.out.println(TextOutputFormat.SEPERATOR);
        //see https://stackoverflow.com/questions/11031785/hadoop-key-and-value-are-tab-separated-in-the-output-file-how-to-do-it-semicol
        job.getConfiguration().set("mapred.textoutputformat.separator", ""); //Prior to Hadoop 2 (YARN)
            job.getConfiguration().set("mapreduce.textoutputformat.separator", "");  //Hadoop v2+ (YARN)
            job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
            job.getConfiguration().set("mapreduce.output.key.field.separator", "");
            job.getConfiguration().set("mapred.textoutputformat.separatorText", ""); // ?
        
            job.setOutputFormatClass(TextOutputFormat.class);
        
        //FileInputFormat.setInputPaths(job, new Path(theJob.getInput().getFile().getLocation()));
        
        Path out = new Path(settings.hdfs_prefix+"/TMP_TABLE_"+theJob.hashCode());
        FileOutputFormat.setOutputPath(job,out);
        
        job.waitForCompletion(verbose);
        theJob.setOutput(new table(new hdfsFile(out), theJob.getOutput().getColNames()));
        
    }
}
