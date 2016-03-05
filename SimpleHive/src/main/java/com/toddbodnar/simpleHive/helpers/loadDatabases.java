/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.helpers;

import com.toddbodnar.simpleHive.IO.fileFile;
import com.toddbodnar.simpleHive.IO.ramFile;
import com.toddbodnar.simpleHive.metastore.database;
import com.toddbodnar.simpleHive.metastore.table;

/**
 *
 * @author toddbodnar
 */
public class loadDatabases {
    public static database singleRow()
    {
        database result = new database("Single Row");
        ramFile r = new ramFile();
        r.append("1,2,3");
        table t1 = new table(r,new String[]{"col_1","col_2","col_3"});
        t1.setSeperator(",");
        result.addTable("table one",t1);
        
        ramFile r2 = new ramFile();
        r2.append("3,2,1");
        table t2 = new table(r2,new String[]{"col_1","col_2","col_3"});
        t2.setSeperator(",");
    
        result.addTable("table two",t2);
        
        
        return result;
    }
    public static database starTrek()
    {
        database result = new database("Star_Trek");
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
    
    /**
     * A DB containing game info from the Battlestar Galactica board game based on data from boardgamegeek.com.
     * 
     * @return 
     */
    public static database battleStarGalacticaGame()
    {
        database result = new database("BSG_Game_Info");
        ramFile r = new ramFile();
        r.append("0\0Base Game");
        r.append("1\0Pegasus");
        r.append("2\0Exodus");
        r.append("3\0Day Break");
        table t = new table(r,new String[]{"id","name"});
        result.addTable("Expansion_Packs", t);
        
        fileFile f = new fileFile("testData/bsg_cards.csv");
        t = new table(f,new String[]{"name","role","leadership","tactics","pilot","engineering","politics","pack"});
        t.setSeperator(",");
        result.addTable( "players",t);
        
        return result;
    }
}
