/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.metastore;

import java.util.HashMap;
import com.toddbodnar.simpleHive.IO.ramFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 *
 * @author toddbodnar
 */
public class database {
    private HashMap<String,table> tables;
    private String name;
    public database(String name)
    {
        tables = new HashMap<String,table>();
        if(name==null)
            name = super.toString();
        this.name = name;
    }
    public database()
    {
        this(null);
    }
    public void addTable(String name, table t)
    {
        tables.put(name, t);
    }
    public table getTable(String name)
    {
        return tables.get(name);
    }
    
    public String toString()
    {
        return name;
    }
    

    String getTableName(table table) {
        for(String key:tables.keySet())
            if(tables.get(key)==table)
                return key;
        
        return "temp table";
    }

    public String showTables() {
        String result = "Database with "+tables.size()+" table(s)\n-------\n";
        for(String name:tables.keySet())
        {
            result+=name+"\n";
        }
        result+="-------";
        return result;
    }
    
    public String toJson()
    {
        String result = "{tables:[";
        for(String name:tables.keySet())
        {
            result+=tables.get(name).toJson(name)+",";
        }
        if(!tables.isEmpty())
            result = result.substring(0,result.length()-1);//truncate trailing comma
        result+="]}";
        return result;
    }
    
    /**
     * Saves the metastore to the local file system
     * @param name the name of the metastore/db
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public void save(String name) throws FileNotFoundException, IOException
    {
        File f = new File(settings.METASTORE_META_LOCATION+File.separatorChar+name);
        FileOutputStream out = new FileOutputStream(f);
        out.write(toJson().getBytes());
        out.close();
    }
}
