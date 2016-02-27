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
public class whereTest {

    private final database db;

    public whereTest() {
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

    @Test
    public void testSingleTest() throws IOException, InterruptedException {
        where theWhere = new where("age > 50");
        theWhere.setInput(db.getTable("people"));

        SimpleHadoopDriver.run(theWhere, true);

        assertEquals("246\0" + "75\0" + "3\0" + "Picard",theWhere.getOutput().getFile().readNextLine());
    }

    @Test
    public void testMultiTest() throws IOException, InterruptedException {
        where theWhere = new where("age > 40 AnD ship = 2");
        theWhere.setInput(db.getTable("people"));

        SimpleHadoopDriver.run(theWhere, true);

        assertEquals("3462\0" + "45\0" + "2\0" + "Tuvok",theWhere.getOutput().getFile().readNextLine());
    }

    @Test
    public void testString() throws IOException, InterruptedException {
        where theWhere = new where("name = 'Tuvok");
        theWhere.setInput(db.getTable("people"));

        SimpleHadoopDriver.run(theWhere, true);

        assertEquals("3462\0" + "45\0" + "2\0" + "Tuvok",theWhere.getOutput().getFile().readNextLine());
    }
}
