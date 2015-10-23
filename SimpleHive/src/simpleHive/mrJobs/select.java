/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs;

import java.util.LinkedList;
import simpleHDFS.ramFile;
import simpleHadoop.context;
import simpleHive.table;

/**
 *
 * @author toddbodnar
 */
public class select extends query{

    private boolean variable[];
    private Object value[];
    private String names[];
    table result;
    table input;
    /**
     * 
     * @param toRetain A string of comma seperated values to retain, either _coln, for column n, or an object, or "x as y" for an object x called y
     */
    public select(String toRetain)
    {
        String split[] = toRetain.split(",");
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
        }
    }
    @Override
    public table getResult() {
        return result;
    }

    @Override
    public void setInput(table in) {
        //no map reduce for a select
        input = in;
    }

    @Override
    public void init(context cont) {
        ramFile r = new ramFile();
        input.reset();
        do
        {
            input.nextRow();
            Object next[] = input.get();
            String result = "";
            boolean first = true;
            for(int ct=0;ct<variable.length;ct++)
            {
                if(first)
                    first=false;
                else
                    result+="\0";
                
                if(variable[ct])
                {
                    result+=next[(int)value[ct]];
                }
                else
                {
                    result+=value[ct];
                }
            }
            r.append(result);
            
            
        }while(input.hasNextRow());
        
        String names[] = new String[variable.length];
        for(int ct=0;ct<names.length;ct++)
        {
            if(this.names[ct]!=null)
            {
                names[ct] = this.names[ct];
            }
            else if(variable[ct])
            {
                names[ct] = input.getColName((int) value[ct]);
            }
            else
            {
                names[ct] = (String)value[ct];
            }
        }
        result = new table(r, names);
    }

    @Override
    public void map(Object input, context cont) {
        //no map reduce for a select
    }

    @Override
    public void reduce(Object key, LinkedList values) {
        //no map reduce for a select
    }
    
}
