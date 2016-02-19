/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.metastore;

import com.toddbodnar.simpleHive.IO.ramFile;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author toddbodnar
 */
public class databaseTest {


    /**
     * Test of toJson method, of class database.
     */
    @Test
    public void testToJson() {
        database instance = new database();
        assertEquals(instance.toJson(),"{tables:[]}");
        
        table t = new table(new ramFile(),new String[]{"col"});
        table a = new table(new ramFile(),new String[]{"column"});
        instance.addTable("test table", t);
        instance.addTable("a table",a);
        String expResult = "{tables:["+t.toJson("test table")+","+a.toJson("a table")+"]}";
        String result = instance.toJson();
        assertEquals(expResult, result);
        
    }
    
}
