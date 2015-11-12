/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs;

import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import simpleHDFS.file;
import simpleHDFS.ramFile;
import simpleHadoop.context;
import simpleHadoop.mrJob;
import simpleHive.table;

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
    public void init(context cont) {
        result = new ramFile();
        input.first();
        while(input.hasNextRow())
        {
            cont.add(input.get());
            input.nextRow();
        }
    }

    @Override
    public void map(Object input, context cont) {
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

   
}
