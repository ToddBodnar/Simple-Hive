/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package helpers;

import simpleHDFS.fileFile;
import simpleHDFS.ramFile;
import simpleHive.database;
import simpleHive.table;

/**
 *
 * @author toddbodnar
 */
public class loadDatabases {
    public static database starTrek()
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
    
    /**
     * A DB containing game info from the Battlestar Galactica board game based on data from boardgamegeek.com.
     * 
     * @return 
     */
    public static database battleStarGalacticaGame()
    {
        database result = new database();
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
