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
import com.toddbodnar.simpleHive.helpers.garbage_collector;
import com.toddbodnar.simpleHive.helpers.settings;
import com.toddbodnar.simpleHive.metastore.table;
import com.toddbodnar.simpleHive.subQueries.join;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * A driver to run mrJobs.
 *
 * @author toddbodnar
 */
public class distributedHadoopDriver {

    /**
     * Runs a job
     *
     * @param theJob the MapReduceJob to be run
     * @param verbose if true, output progress information
     */
    public static void run(MapReduceJob theJob, boolean verbose) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = GetConfiguration.get();
        Job job = Job.getInstance(conf, theJob.toString());
        job.setJarByClass(distributedHadoopDriver.class);

        job.setMapperClass(theJob.getMapper().getClass());
        job.setReducerClass(theJob.getReducer().getClass());

        job.setMapOutputKeyClass(theJob.getKeyType());
        job.setMapOutputValueClass(theJob.getValueType());

        theJob.writeConfig(job.getConfiguration());

        hdfsFile input = hdfsFile.transferToHDFS(theJob.getInput().getFile());
        if (!input.equals(theJob.getInput().getFile())) {
            garbage_collector.noteCreated(input);
        }
        if (theJob.getClass().equals(join.class)) {
            join jobLeftJoin = (join) theJob;

            hdfsFile input2 = hdfsFile.transferToHDFS(jobLeftJoin.getOtherInput().getFile());
            if (!input2.equals(jobLeftJoin.getOtherInput().getFile())) {
                garbage_collector.noteCreated(input2);
            }

            Mapper maps[] = jobLeftJoin.getMapperPairs();
            MultipleInputs.addInputPath(job, input.getPath(), TextInputFormat.class, maps[0].getClass());
            MultipleInputs.addInputPath(job, input2.getPath(), TextInputFormat.class, maps[1].getClass());
        } else {
            MultipleInputs.addInputPath(job, input.getPath(), TextInputFormat.class);
        }

        job.getConfiguration().set(TextOutputFormat.SEPERATOR, "");

        job.setOutputFormatClass(TextOutputFormat.class);

        //FileInputFormat.setInputPaths(job, new Path(theJob.getInput().getFile().getLocation()));
        Path out = new Path(settings.hdfs_prefix + "/TMP_TABLE_" + theJob.hashCode());
        FileOutputFormat.setOutputPath(job, out);

        boolean success = job.waitForCompletion(true);

        if (!success) {
            System.err.println("Error processing " + theJob);
            return;
        }

        FileSystem fs = FileSystem.get(GetConfiguration.get());

        fs.delete(new Path(out, "_SUCCESS"), false);

        table output = new table(new hdfsFile(out), theJob.getOutput().getColNames());
        output.setSeperator(theJob.getOutput().getSeperator());

        theJob.setOutput(output);
        
        garbage_collector.noteCreated(output.getFile());
    }
}
