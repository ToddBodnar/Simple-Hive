/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import java.util.ArrayList;
import java.util.LinkedList;
import com.toddbodnar.simpleHive.IO.ramFile;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHive.metastore.table;

/**
 *
 * @author toddbodnar
 */
public class leftJoin extends query{
    /**
     * Joins two tables
     * @param other the other table to join
     * @param key the location of the key in the table set by setInput
     * @param otherkey the location of the key in the table, other
     */
    public leftJoin(table other, int key, int otherkey)
    {
        mainKey=key;
        this.otherKey=otherKey;
        this.other=other;
    }
    
    public leftJoin(int key, int otherkey)
    {
        this(null,key,otherkey);
    }
    
    public void setOther(table other)
    {
        this.other=other;
    }
    
    table main,other;
    ramFile storage;
    int mainKey,otherKey;
    @Override
    public table getResult() {
        String names[] = new String[main.getColNames().length+other.getColNames().length];
        for(int ct=0;ct<main.getColNames().length;ct++)
        {
            names[ct] = main.getColName(ct);
        }
        for(int ct=0;ct<other.getColNames().length;ct++)
        {
            names[main.getColNames().length+ct] = other.getColName(ct);
        }
        return new table(storage,names);
    }

    @Override
    public void setInput(table in) {
        main=in;
    }

    @Override
    public void inputFormat(simpleContext cont) {
        storage = new ramFile();
        main.reset();
        other.reset();
        while(main.hasNextRow())
        {
            main.nextRow();
            Object o[] = main.get();
            cont.add(new Object[]{1,o});
        }
        while(other.hasNextRow())
        {
            other.nextRow();
            Object o[] = other.get();
            cont.add(new Object[]{2,o});
        }
    }

    @Override
    public void map(Object input, simpleContext cont) {
        int id = (int)(((Object[])input)[0]);
        Object row[] = (Object[])(((Object[])input)[1]);
        if(id==1)
        {
            cont.emit(row[mainKey], new Object[]{1,row});
        }
        else
        {
            cont.emit(row[otherKey], new Object[]{2,row});
        }
    }

    @Override
    public void reduce(Object key, LinkedList values) {
        Object left = null;
        ArrayList right = new ArrayList();
        for(Object o:values)
        {
            Object o2[] = (Object[])o;
            if((int)o2[0]==2)
                left = o2[1];
            else
                right.add(o2[1]);
        }
        if(left==null || right==null)
            return;
        for(Object rightO:right)
        {
        String result = "";
        boolean first = true;
        for(Object o:(Object[])rightO)
        {
            if(first)
                first=false;
            else
                result+="\0";
            result+=o;
        }
        for(Object o:(Object[])left)
        {
            if(first)
                first=false;
            else
                result+="\0";
            result+=o;
        }
        
        storage.append(result);
        }
    }
    
    public table getInput() {
        return main;
    }

    @Override
    public table getOutput() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setOutput(table table) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
