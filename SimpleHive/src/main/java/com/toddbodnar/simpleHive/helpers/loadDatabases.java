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
        r.append("1234\001"+"23\001"+"1\001"+"Kirk");
        r.append("2345\001"+"45\001"+"1\001"+"Spock");
        r.append("3456\001"+"34\001"+"1\001"+"Bones");
        r.append("4353\001"+"35\001"+"2\001"+"Janeway");
        r.append("3462\001"+"45\001"+"2\001"+"Tuvok");
        r.append("246\001"+"75\001"+"3\001"+"Picard");
        r.append("2364\001"+"8\001"+"3\001"+"Data");
        table t = new table(r,new String[]{"id","age","ship","name"});
        t.setSeperator("\001");
        result.addTable("people", t);
        
        
        r = new ramFile();
        r.append("1\001"+"Enterprise");
        r.append("2\001"+"Voyager");
        r.append("3\001"+"Enterprise D");
        
        t = new table(r,new String[]{"shipID","shipname"});
        t.setSeperator("\001");
        
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
