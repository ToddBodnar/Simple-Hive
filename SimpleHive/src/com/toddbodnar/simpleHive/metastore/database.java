/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.metastore;

import java.util.HashMap;
import com.toddbodnar.simpleHive.IO.ramFile;

/**
 *
 * @author toddbodnar
 */
public class database {
    private HashMap<String,table> tables;
    public database()
    {
        tables = new HashMap<String,table>();
    }
    public void addTable(String name, table t)
    {
        tables.put(name, t);
    }
    public table getTable(String name)
    {
        return tables.get(name);
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
}
