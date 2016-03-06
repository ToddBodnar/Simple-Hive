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
import java.util.LinkedList;
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
public class colStatsTest {
    private final database db;
    public colStatsTest() {
        db = loadDatabases.starTrek();
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

    /**
     * Test stats for ages of each person
     */
    @Test
    public void testStats() throws IOException, InterruptedException
    {
        colStats cs = new colStats(1,-1);
        cs.setInput(db.getTable("people"));
        SimpleHadoopDriver.run(cs, true);
        
        assertEquals("*"+"\001"+75.0 + "\001" + 8.0 + "\001" + (265/7.0) + "\001" + 265.0 + "\001" + 7,cs.getOutput().getFile().readNextLine());
    }
    
    /**
     * Test stats for ages of each person, grouped by their ship id
     */
    @Test
    public void testStatsGroup() throws IOException, InterruptedException
    {
        colStats cs = new colStats(1,2);
        cs.setInput(db.getTable("people"));
        SimpleHadoopDriver.run(cs, true);
        String in;
        boolean notests = true;
        while((in = cs.getOutput().getFile().readNextLine())!=null)
        {
            notests = false;
            switch(in.charAt(0))
            {
                case 1:
                    assertEquals("1"+"\001"+45.0 + "\001" + 23.0 + "\001" + (102/3.0) + "\001" + 102.0 + "\001" + 3,in);
                    break;
                case 2:
                    assertEquals("2"+"\001"+45.0 + "\001" + 35.0 + "\001" + (80/2.0) + "\001" + 80.0 + "\001" + 2,in);
                    break;
                case 3:
                    assertEquals("3"+"\001"+75.0 + "\001" + 8.0 + "\001" + (83/2.0) + "\001" + 83.0 + "\001" + 2,in);
                    break;
            }
        }
        assert(!notests);
    }
}
