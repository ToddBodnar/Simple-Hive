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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author toddbodnar
 */
public class select extends query<Text,Text>{

    private boolean variable[];
    private Object value[];
    private String names[];
    private String query;
    private boolean passThrough = false;

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


    public void parseInput() {
        //no map reduce for a select
        
        if(query.trim().equals("*"))
        {
            passThrough = true;
            names = getInput().getColNames();
            return;
        }
        String split[] = query.split(",");
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
            if(getInput().getColNum(split[ct].trim())!=-1)
            {
                variable[ct] = true;
                value[ct] = getInput().getColNum(split[ct].trim());
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

//    public void inputFormat(simpleContext cont) {
//        if(query.trim().equals("*"))
//        {
//            result = input;
//            return;
//        }
//        ramFile r = new ramFile();
//        input.reset();
//        do
//        {
//            input.nextRow();
//            Object next[] = input.get();
//            String result = "";
//            boolean first = true;
//            for(int ct=0;ct<variable.length;ct++)
//            {
//                if(first)
//                    first=false;
//                else
//                    result+="\0";
//                
//                if(variable[ct])
//                {
//                    result+=next[(int)value[ct]];
//                }
//                else
//                {
//                    result+=value[ct];
//                }
//            }
//            r.append(result);
//            
//            
//        }while(input.hasNextRow());
//        
//        String names[] = new String[variable.length];
//        for(int ct=0;ct<names.length;ct++)
//        {
//            if(this.names[ct]!=null)
//            {
//                names[ct] = this.names[ct];
//            }
//            else if(variable[ct])
//            {
//                names[ct] = input.getColName((int) value[ct]);
//            }
//            else
//            {
//                names[ct] = (String)value[ct];
//            }
//        }
//        result = new table(r, names);
//    }
//    
    public String toString()
    {
        String result = "select ";
        if(query.trim().equals("*"))
            return result + "*"+"from "+(getInput()==null?"null":getInput().toString());
        String names[] = new String[variable.length];
        for(int ct=0;ct<names.length;ct++)
        {
            if(this.names[ct]!=null)
            {
                names[ct] = this.names[ct];
            }
            else if(variable[ct])
            {
                names[ct] = getInput().getColName((int) value[ct]);
            }
            else
            {
                names[ct] = (String)value[ct];
            }
        }
        
        for(String n:names)
            result+=n+" ";
        return result+"from "+(getInput()==null?"null":getInput().toString());
    }

    @Override
    public Mapper getMapper() {
        return new Mapper<IntWritable[],Text,Text,Text>()
                {
                    {
                        parseInput();
                    }
                    public void map(IntWritable key[], Text line, Context cont) throws IOException, InterruptedException
                {
                    if(passThrough)
                {
                    cont.write(line,null);
                    return;
                }
                    
                    Object next[] = line.toString().split(getInput().getSeperator());
            String result = "";
            boolean first = true;
            for(int ct=0;ct<variable.length;ct++)
            {
                if(first)
                    first=false;
                else
                    result+=getInput().getSeperator();
                
                if(variable[ct])
                {
                    result+=next[(int)value[ct]];
                }
                else
                {
                    result+=value[ct];
                }
            }
            cont.write( new Text(result),null);
                }
                
                };
    }

    @Override
    public Reducer getReducer() {
        return new Reducer()
                {
                    //just default behavior, since all processing is done map side
                };
    }
    
    public void setInput(table in)
    {
        super.setInput(in);
        parseInput();
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
        return Text.class;
    }


    
   
    
}
