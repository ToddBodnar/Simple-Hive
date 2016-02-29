/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import java.util.LinkedList;
import com.toddbodnar.simpleHive.IO.ramFile;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author toddbodnar
 */
public class select extends query<Text,Object>{

    private String names[];
    private String query;

    /**
     * 
     * @param toRetain A string of comma seperated values to retain, either _coln, for column n, or an object, or "x as y" for an object x called y
     */
    public select(String toRetain)
    {
        query = toRetain;
        
        /**
         *  //this code assumes a preparsed variable list, moved this down to the setInput command to have a JIT processing
         * String split[] = toRetain.split(",");
        variable = new boolean[split.length];
        value = new Object[split.length];
        names = new String[split.length];
        for(int ct=0;ct<split.length;ct++)
        {
            if(split[ct].contains(" as "))
            {
                int start = -1;
                for(int i=0;i<split[ct].length();i++)
                {
                    if(split[ct].substring(i).startsWith(" as "))
                    {
                        start = i;
                        break;
                    }
                }
                names[ct]=split[ct].substring(start+4);
                split[ct] = split[ct].substring(0, start);
            }
            if(split[ct].startsWith("_col"))
            {
                variable[ct] = true;
                value[ct] = Integer.parseInt(split[ct].substring(4));
            }
            else
            {
                variable[ct] = false;
                value[ct] = split[ct];
            }
        }**/
    }


    public String toString()
    {
        return query+"from "+(getInput()==null?"null":getInput().toString());
    }

    @Override
    public Mapper getMapper() {
        return new selectMapper();
    }
    
    private static class selectMapper extends Mapper<Object,Text,Text,Object>
                {
        private boolean passThrough;
        public String names[];
        private Object[] value;
        private boolean[] variable;
                    public void parseInput(Configuration conf) {
        //no map reduce for a select
        
        if(conf.get("SIMPLE_HIVE.SELECT.QUERY_STR").trim().equals("*"))
        {
            passThrough = true;
            names = conf.getStrings("SIMPLE_HIVE.SELECT.INPUT_COL_NAMES");
            return;
        }
        String split[] = conf.get("SIMPLE_HIVE.SELECT.QUERY_STR").split(",");
        variable = new boolean[split.length];
        value = new Object[split.length];
        names = new String[split.length];
        for(int ct=0;ct<split.length;ct++)
        {
            if(split[ct].contains(" as "))
            {
                int start = -1;
                for(int i=0;i<split[ct].length();i++)
                {
                    if(split[ct].substring(i).startsWith(" as "))
                    {
                        start = i;
                        break;
                    }
                }
                names[ct]=split[ct].substring(start+4).trim();
                split[ct] = split[ct].substring(0, start).trim();
            }
            if(new table(new ramFile(), conf.getStrings("SIMPLE_HIVE.SELECT.INPUT_COL_NAMES")).getColNum(split[ct].trim())!=-1)
            {
                variable[ct] = true;
                value[ct] = new table(new ramFile(),conf.getStrings("SIMPLE_HIVE.SELECT.INPUT_COL_NAMES")).getColNum(split[ct].trim());
                if(names[ct]==null)
                    names[ct] = split[ct].trim();
            }
            else
            {
                variable[ct] = false;
                value[ct] = split[ct];
                if(names[ct]==null)
                    names[ct] = ""+value[ct];
            }
        }
    }
                    
                    public void setup(Context cont)
                    {
                        
                        parseInput(cont.getConfiguration());
                    }
                    @Override
                    public void map(Object key, Text line, Context cont) throws IOException, InterruptedException
                {
                    if(passThrough)
                {
                    cont.write(line,NullWritable.get());
                    return;
                }
                    
                    Object next[] = line.toString().split(cont.getConfiguration().get("SIMPLE_HIVE.SELECT.INPUT_SEPERATOR"));
            String result = "";
            boolean first = true;
            for(int ct=0;ct<variable.length;ct++)
            {
                if(first)
                    first=false;
                else
                    result+=cont.getConfiguration().get("SIMPLE_HIVE.SELECT.INPUT_SEPERATOR");
                
                if(variable[ct])
                {
                    result+=next[(int)value[ct]];
                }
                else
                {
                    result+=value[ct];
                }
            }
            cont.write( new Text(result),NullWritable.get());
                }
                
                }

    @Override
    public Reducer getReducer() {
        return new SelectReducer();
    }
    
    private static class SelectReducer extends Reducer
                {
                    //just default behavior, since all processing is done map side
                };
    
    public void setInput(table in)
    {
        super.setInput(in);
        Configuration conf = new Configuration();
        this.writeConfig(conf);
        selectMapper m = new selectMapper();
        m.parseInput(conf);
        names = m.names;
    }
    
    public table getOutput()
    {
        if(super.getOutput()!=null)
            return super.getOutput();
        table result = new table(new ramFile(),names);
        result.setSeperator(getInput().getSeperator());
        super.setOutput(result);
        return result;
    }

    @Override
    public Class getKeyType() {
        return Text.class;
    }

    @Override
    public Class getValueType() {
        return NullWritable.class;
    }

    @Override
    public void writeConfig(Configuration conf) {
        conf.set("SIMPLE_HIVE.SELECT.QUERY_STR", query);
        conf.set("SIMPLE_HIVE.SELECT.INPUT_SEPERATOR", getInput().getSeperator());
        conf.setStrings("SIMPLE_HIVE.SELECT.INPUT_COL_NAMES", getInput().getColNames());
    }


    
   
    
}
