/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHadoop;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import com.toddbodnar.simpleHadoop.simpleContext;

/**
 *
 * @author toddbodnar
 */
public class mrJobHadoopWrapper{
    public static class map extends Mapper<Object,Text,Object,Object>
    {
        public void map(Object key, Text line, Context context)
        {
            mrJob theJob = null;
            try {
                 theJob = (mrJob) com.toddbodnar.simpleHive.helpers.serialString.fromString(context.getConfiguration().get("theMRJob"));
            } catch (Exception ex) {
                Logger.getLogger(mrJobHadoopWrapper.class.getName()).log(Level.SEVERE, null, ex);
               
            }
            theJob.map(line, new contextWrapper(context));
        }
        
        private class contextWrapper extends simpleContext
        {
            public contextWrapper(Mapper.Context theContext)
            {
                context = theContext;
            }
            public void emit(Object key, Object value)
            {
                try {
                    context.write(key, value);
                } catch (IOException ex) {
                    Logger.getLogger(mrJobHadoopWrapper.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InterruptedException ex) {
                    Logger.getLogger(mrJobHadoopWrapper.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            Mapper.Context context;
        }
    }
    
    public static class reduce extends Reducer<Object,Object,Text,Object>
    {
        public void reduce(Object key, Text line, Reducer.Context context)
        {
            
        }
        private class contextWrapper extends simpleContext
        {
            public contextWrapper(Reducer.Context theContext)
            {
                context = theContext;
            }
            public void emit(Object key, Object value)
            {
                try {
                    
                    context.write(key, value);
                } catch (IOException ex) {
                    Logger.getLogger(mrJobHadoopWrapper.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InterruptedException ex) {
                    Logger.getLogger(mrJobHadoopWrapper.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            Reducer.Context context;
        }
    }
    
    
}
