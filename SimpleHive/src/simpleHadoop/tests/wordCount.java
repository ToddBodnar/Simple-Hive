/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHadoop.tests;

import com.toddbodnar.simpleHive.helpers.tests;
import java.util.LinkedList;
import com.toddbodnar.simpleHive.IO.file;
import com.toddbodnar.simpleHive.IO.laggyRamFile;
import com.toddbodnar.simpleHive.IO.ramFile;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHadoop.SimpleHadoopDriver;
import com.toddbodnar.simpleHadoop.MapReduceJob;
import com.toddbodnar.simpleHadoop.tableRecordReader;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * inputFormat: read in the file and pass each paragraph to a mapper
 map: split each paragraph apart and send each word to a reducer
 reducer: count the number of each word, store results in the file wordCount.input
 * @author toddbodnar
 */
public class wordCount extends MapReduceJob<IntWritable[],Text,Text,LongWritable,Text,LongWritable>{



 
    
    table input;
    table result;

    
    public int test(boolean verbose) throws IOException, InterruptedException {
        ramFile testFile = new laggyRamFile(10);
        for(String s:testData)
            testFile.append(s);
        boolean crash = false;
        wordCount job = new wordCount();
        job.setInput(new table(testFile,null));
        job.setOutput(new table(new ramFile(),null));
        
        SimpleHadoopDriver.run(job,true);
        
        
        int score = tests.score(verbose,!crash,"Running job");
        
        
        
        int googleCount=-1;
        int hadoopCount=-1;
        job.getOutput().reset();
        if (!crash) {
            while (job.getOutput().getFile().hasNext()) {
                String line = job.getOutput().getFile().readNextLine();
                //System.out.println(line);
                if (line.split("\0")[0].equals("google")) {
                    googleCount = Integer.parseInt(line.split("\0")[1]);
                }
                if (line.split("\0")[0].equals("hadoop")) {
                    hadoopCount = Integer.parseInt(line.split("\0")[1]);
                }
            }
        }
        //    System.out.println(job.result.readNextLine());
        score+=tests.score(verbose, googleCount==3, "Testing if correct number of occurances of Google(3)");
        score+=tests.score(verbose, hadoopCount==10, "Testing if correct number of occurances of Hadoop(10)");
        
        
        return score;
    }
    
    public static void main(String args[])
    {
        try {
            new wordCount().test(true);
        } catch (IOException ex) {
            Logger.getLogger(wordCount.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(wordCount.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * From Wikipedia's entry on "Apache Hadoop"
     */
    String[] testData = {"Apache Hadoop is an open-source software framework written in Java for distributed storage and distributed processing of very large data sets on computer clusters built from commodity hardware. All the modules in Hadoop are designed with a fundamental assumption that hardware failures (of individual machines, or racks of machines) are commonplace and thus should be automatically handled in software by the framework.[3]",
"The core of Apache Hadoop consists of a storage part (Hadoop Distributed File System (HDFS)) and a processing part (MapReduce). Hadoop splits files into large blocks and distributes them amongst the nodes in the cluster. To process the data, Hadoop MapReduce transfers packaged code for nodes to process in parallel, based on the data each node needs to process. This approach takes advantage of data locality[4]—nodes manipulating the data that they have on hand—to allow the data to beprocessed faster and more efficiently than it would be in a more conventional supercomputer architecture that relies on a parallel file system where computation and data are connected via high-speed networking.[5]" +
"The base Apache Hadoop framework is composed of the following modules:",
"	•	Hadoop Common – contains libraries and utilities needed by other Hadoop modules;",
"	•	Hadoop Distributed File System (HDFS) – a distributed file-system that stores data on commodity machines, providing very high aggregate bandwidth across the cluster;",
"	•	Hadoop YARN – a resource-management platform responsible for managing computing resources in clusters and using them for scheduling of users' applications;[6][7] and",
"	•	Hadoop MapReduce – a programming model for large scale data processing." ,
"The term \"Hadoop\" has come to refer not just to the base modules above, but also to the \"ecosystem\",[8] or collection of additional software packages that can be installed on top of or alongside Hadoop, such as Apache Pig, Apache Hive, Apache HBase, Apache Phoenix, Apache Spark, Apache Zookeeper, Impala, Apache Flume, Apache Sqoop,Apache Oozie, Apache Storm and others.[9]" ,
"Apache Hadoop's MapReduce and HDFS components were inspired by Google papers on their MapReduce and Google File System.[10]" ,
"The Hadoop framework itself is mostly written in the Java programming language, with some native code in C and command line utilities written as Shell script. For end-users, though MapReduce Java code is common, any programming language can be used with \"Hadoop Streaming\" to implement the \"map\" and \"reduce\" parts of the user's program.[11] Other related projects expose other higher-level user interfaces." ,
"Prominent corporate users of Hadoop include Facebook and Yahoo. It can be deployed in traditional on-site datacenters but has also been implemented in public cloud spaces such as Microsoft Azure, Amazon Web Services, Google Compute Engine, and IBM Bluemix." ,
"Apache Hadoop is a registered trademark of the Apache Software Foundation."};
   
    public int numTests() {
        return 3;
    }

    @Override
    public void setInput(table in) {
        input = in;
    }

    @Override
    public table getInput() {
        return input;
    }

    @Override
    public table getOutput() {
        return result;
    }

    @Override
    public void setOutput(table table) {
        result = table;
    }

    @Override
    public RecordReader<IntWritable[], Text> getRecordReader() {
        return new tableRecordReader(getInput());
    }

    @Override
    public Mapper<IntWritable[], Text, Text, LongWritable> getMapper() {
        return new Mapper<IntWritable[], Text, Text, LongWritable>() {
            public void map(IntWritable key[], Text value, Context context) throws IOException, InterruptedException {
                //value = value.toLowerCase();
                for (String token : value.toString().toLowerCase().split(" ")) {
                        context.write(new Text(token), new LongWritable(1));
                    
                }
            }
        };
    }

    @Override
    public Reducer<Text, LongWritable,Text, LongWritable> getReducer() {
        return new Reducer<Text, LongWritable,Text, LongWritable>() {
            public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
                long sum = 0;
                for (LongWritable l : values) {
                    sum += l.get();
                }
                context.write(key, new LongWritable(sum));
            }
        };
    }

    @Override
    public Class getKeyType() {
        return Text.class;
    }

    @Override
    public Class getValueType() {
        return LongWritable.class;
    }
}
