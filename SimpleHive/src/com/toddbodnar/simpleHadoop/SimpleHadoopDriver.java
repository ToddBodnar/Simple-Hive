/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHadoop;

import com.toddbodnar.simpleHive.IO.file;
import com.toddbodnar.simpleHive.IO.ramFile;
import com.toddbodnar.simpleHive.helpers.pair;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A driver to run mrJobs.
 * @author toddbodnar
 */
public class SimpleHadoopDriver {
    private static long lastUpdate = -1;
    
    /**
     * @deprecated 
     * Log the current map-reduce job, but only when a log hasn't been recently executed
     * @param mapCt
     * @param reduceCt
     * @param totalMap
     * @param totalReduce
     * @param doingMap
     * @param verbose 
     */
    private static void log(int mapCt, int reduceCt, int totalMap, int totalReduce, boolean doingMap, boolean verbose)
    {
        if(System.currentTimeMillis() - lastUpdate < 500)
            return;
        if(!verbose)
            return;
        
        if(doingMap)
        {
            if((100*mapCt/totalMap)==(100*(mapCt-1)/totalMap))
                return;
        }
        else
        {
            if(totalReduce==0 || (100*reduceCt/totalReduce)==(100*(reduceCt-1)/totalReduce))
                return;
        }
        lastUpdate = System.currentTimeMillis();
        System.out.println(System.currentTimeMillis()+"\tMap "+(100*mapCt/totalMap)+"% \tReduce "+(100*reduceCt/totalReduce)+"%");
    }
    
    /**
     * Runs a job
     * @param theJob the MapReduceJob to be run
     * @param verbose if true, output progress information
     */
    public static void run(MapReduceJob theJob, boolean verbose) throws IOException, InterruptedException
    {
        simpleContext cont = new simpleContext(theJob.getRecordReader(),theJob.getKeyType(),theJob.getValueType());
        //if(verbose)
          //  System.out.println("Init "+theJob.getClass().toString());
        
        
        
        
        theJob.getMapper().run((Mapper.Context) cont.getMapContext());
        
        //if(verbose)
          //  System.out.println("Reduce "+theJob.getClass().toString());
        theJob.getReducer().run((Reducer.Context) cont.getReduceContext());

        
        
        file out = theJob.getOutput().getFile();
        
        for(Object p:cont.getResults())
        {
            pair thePair = (pair)p;
           
            out.append(thePair.getKey()+"\0"+thePair.getValue().toString());
        }
    }
}
