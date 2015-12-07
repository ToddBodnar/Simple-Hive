/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs;

import java.util.LinkedList;
import simpleHDFS.ramFile;
import simpleHadoop.simpleContext;
import simpleHive.table;

/**
 *
 * @author toddbodnar
 */
public class colStats extends query{
    public colStats(int statsCol)
    {
        this(statsCol,-1);
    }
    /**
     * Constructs a stats job for column statsCol, optimally grouped by groupBy
     * @param statsCol
     * @param groupBy 
     */
    public colStats(int statsCol, int groupBy)
    {
        groupByCol=groupBy;
        this.statsCol=statsCol;
        
        resultStorage = new ramFile();
        result = new table(resultStorage, stats );
    }
    
    @Override
    public table getResult() {
        return result;
    }

    @Override
    public void setInput(table in) {
        inTable = in;
    }


    @Override
    public void map(Object input, simpleContext cont) {
        Object row[] = (Object[])input;
        
        String key = "*";
        if(groupByCol != -1)
        {
            key = row[groupByCol].toString();
        }
        
        cont.emit(key, row[statsCol]);
    }

    @Override
    public void reduce(Object key, LinkedList values) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0;
        double ct = 0;
        
        for(Object value:values)
        {
            double number = Double.parseDouble((String) value);
            if(min>number)
                min=number;
            if(max<number)
                max=number;
            sum+=number;
            ct++;
        }
        
        double avg = sum/ct;
        
        String result = max+"\0"+min+"\0"+avg+"\0"+sum+"\0"+ct+"\0"+key;
        
        resultStorage.append(result);
    }
    
    private int groupByCol, statsCol;
    private table result,inTable;
    private ramFile resultStorage;
    private static String stats[] = new String[]{"max","min","avg","sum","count","group"};

    @Override
    public table getInput() {
        return inTable;
    }
}
