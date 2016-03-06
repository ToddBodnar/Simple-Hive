/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import com.toddbodnar.simpleHadoop.SimpleHadoopDriver;
import com.toddbodnar.simpleHive.helpers.loadDatabases;
import com.toddbodnar.simpleHive.metastore.database;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class joinTest {
    private database db;
    public joinTest()
    {
        db = loadDatabases.singleRow();
    }
    
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void goodJoinTestMatchingSeparators() throws IOException, InterruptedException
    {
        join left = new join(1,1);
        left.setInput(db.getTable("table one"));
        left.setOtherInput(db.getTable("table two"));
        
        SimpleHadoopDriver.run(left, true);
        
        assertEquals("1,2,3"+","+"3,2,1",left.getOutput().getFile().readNextLine());
    }
    
    @Test
    public void goodJoinTestNonMatchingSeparators() throws IOException, InterruptedException
    {
        join left = new join(1,1);
        left.setInput(db.getTable("table one"));
        left.setOtherInput(db.getTable("table two tab separated"));
        
        SimpleHadoopDriver.run(left, true);
        
        assertEquals("1\0012\0013"+"\001"+"3\0012\0011",left.getOutput().getFile().readNextLine());
    }
    
    @Test
    public void noMatchJoinTest() throws IOException, InterruptedException
    {
        join left = new join(0,0);
        left.setInput(db.getTable("table one"));
        left.setOtherInput(db.getTable("table two"));
        
        SimpleHadoopDriver.run(left, true);
        
        assertEquals(null,left.getOutput().getFile().readNextLine());
    }
    
}
