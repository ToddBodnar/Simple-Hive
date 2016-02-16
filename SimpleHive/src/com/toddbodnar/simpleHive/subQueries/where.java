/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.toddbodnar.simpleHive.IO.file;
import com.toddbodnar.simpleHive.IO.ramFile;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHadoop.MapReduceJob;
import com.toddbodnar.simpleHive.metastore.table;

/**
 * 
 * @author toddbodnar
 */
public class where extends query{

    booleanTest theQuery;
    String partialQuery;
    /**
     * given a partial query (the part after the where) of the form:
     * _col1 < "string" AND _col2 = "string" AND _col3 = "string"
     * @param partialQuery 
     */
    public where(String partialQuery)
    {
        this.partialQuery = partialQuery; 
        
        result = new ramFile();
    }
    private table input;
    private file result;
    @Override
    public table getResult() {
        return new table(result,input.getColNames());
    }

    @Override
    public void setInput(table in) {
        
        input = in;
       
        //replace the variables in the query string with the actual column numbers
        String split[] = partialQuery.split(" ");
        for(int ct=0;ct<split.length;ct++)
        {
            int colNum = input.getColNum(split[ct]);
            if(colNum!=-1)
                split[ct] = "_col"+colNum;
        }
        partialQuery = split[0];
        for(int ct=1;ct<split.length;ct++)
            partialQuery+=" "+split[ct];
        
        theQuery = new booleanTest(partialQuery);
        
    }



    @Override
    public void map(Object input, simpleContext cont) {
        try {
            if(theQuery.evaluate((Object[])input))
            {
                String res = "";
                boolean first = true;
                for(Object o:(Object[])input)
                {
                    if(first)
                        first=false;
                    else
                        res+="\0";
                    res+=o;
                    //System.out.println(o);
                }
                result.append(res);
            }
        } catch (Exception ex) {
            Logger.getLogger(where.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void reduce(Object key, LinkedList values) {
        //there are no reduce operators for a where
    }

    public table getInput() {
        return input;
    }
    
    @Override
    public table getOutput() {
        return getResult();    
    }

    @Override
    public void setOutput(table table) {
        result = table.getFile();
    }
   
}
