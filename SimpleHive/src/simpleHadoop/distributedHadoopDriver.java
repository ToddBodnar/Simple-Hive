/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * A driver to run mrJobs.
 * @author toddbodnar
 */
public class distributedHadoopDriver {

    /**
     * Runs a job
     * @param theJob the mrJob to be run
     * @param verbose if true, output progress information
     */
    public static void run(mrJob theJob, boolean verbose) throws IOException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, theJob.toString());
        job.setJarByClass(distributedHadoopDriver.class);
        
        job.setMapperClass(mrJobHadoopWrapper.map.class);
        job.setReducerClass(mrJobHadoopWrapper.reduce.class);
        
        
        
        
    }
}
