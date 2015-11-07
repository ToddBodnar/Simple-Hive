/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive;

import java.util.HashMap;
import simpleHDFS.ramFile;

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
    
    
    public static database getTestDB()
    {
        database result = new database();
        ramFile r = new ramFile();
        r.append("1234\0"+"23\0"+"1\0"+"Kirk");
        r.append("2345\0"+"45\0"+"1\0"+"Spock");
        r.append("3456\0"+"34\0"+"1\0"+"Bones");
        r.append("4353\0"+"35\0"+"2\0"+"Janeway");
        r.append("3462\0"+"45\0"+"2\0"+"Tuvok");
        r.append("246\0"+"75\0"+"3\0"+"Picard");
        r.append("2364\0"+"8\0"+"3\0"+"Data");
        table t = new table(r,new String[]{"id","age","ship","name"});
        result.addTable("people", t);
        
        
        r = new ramFile();
        r.append("1\0"+"Enterprise");
        r.append("2\0"+"Voyager");
        r.append("3\0"+"Enterprise D");
        
        t = new table(r,new String[]{"shipID","shipname"});
        result.addTable("ships",t);
        return result;
    }

    String getTableName(table table) {
        for(String key:tables.keySet())
            if(tables.get(key)==table)
                return key;
        
        return "temp table";
    }
}
