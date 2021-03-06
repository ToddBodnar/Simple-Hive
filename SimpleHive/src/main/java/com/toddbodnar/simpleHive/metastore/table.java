/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.metastore;

import com.toddbodnar.simpleHive.helpers.settings;
import com.toddbodnar.simpleHive.IO.file;

/**
 * Stores the metadata (colum names, etc) and iterators for a table
 * @author toddbodnar
 */
public class table {
    /**
     * Default constructor
     * @param store The simpleHDFS.file where the table is stored
     * @param names Column names
     */
    public table(file store, String names[])
    {
        storage = store;
        colNames = names;
        first();
    }
    
    public String toJson(String name)
    {
        String result = "{name:\""+name+"\",columns:[";
        for(int col=0;col<colNames.length;col++)
        {
            result+="\""+colNames[col]+"\"";
            if(col<colNames.length-1)
                result+=",";
        }
        result+="],datastore:"+storage.toJson()+"}";
        return result;
    }
    private String colNames[],row[];
    private file storage;
    public String getColName(int col)
    {
        return colNames[col];
    }
    public int getColNum(String name)
    {
        for(int ct=0;ct<colNames.length;ct++)
            if(colNames[ct].equalsIgnoreCase(name))
                return ct;
        
        return -1;
    }
    
    public Object get(int col)
    {
        return row[col];
    }
    
    public void nextRow()
    {
        if(storage.hasNext())
            row = storage.readNextLine().split(seperator);
        else
            row = null;
    }
    
    public boolean hasNextRow()
    {
        return storage.hasNext();
    }
    
    public void first()
    {
        storage.resetStream();
        nextRow();
    }

    public Object[] get() {
        Object result[] = new Object[row.length];
        for(int ct=0;ct<result.length;ct++)
            result[ct] = get(ct);
        return result;
    }

    public String[] getColNames() {
        return colNames;
    }
    
    public String print()
    {
        String result = "";
        for(int ct=0;ct<colNames.length;ct++)
        {
            result+=colNames[ct]+"\t";
        }
        result+="\n";
        for(int ct=0;ct<colNames.length;ct++)
        {
            result+="-----\t";
        }
        result+="\n";
        first();
        if(row == null)
            return result;
        for(int ct=0;ct<100;ct++)
        {
            
            for(int ct2=0;ct2<colNames.length;ct2++)
            {
                result+=get(ct2)+"\t";
            }
            result+="\n";
            if(!hasNextRow())
            {
                break;
            }
            nextRow();
        }
        return result;
    }

    public void reset() {
        storage.resetStream();
    }

    public String toString() {
        String name;
        if(settings.currentDB!=null)
            name = settings.currentDB.getTableName(this);
        else
            name = "unknown table";
        return "Table "+name+" with "+getColNames().length+" columns";
    }
    
    public String describe(){
        String result = toString();
        result+="\n-------";
        for(String var:getColNames())
            result+="\n"+var;
        return result+"\n-------";
    }
    
    public String getSeperator()
    {
        return seperator;
    }
    
    public void setSeperator(String sepChar)
    {
        seperator = sepChar;
    }
    
    private String seperator = "\0";

    public file getFile() {
        return storage;
    }
}
