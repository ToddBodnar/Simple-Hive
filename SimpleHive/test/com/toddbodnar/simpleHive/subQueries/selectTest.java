/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import com.toddbodnar.simpleHadoop.SimpleHadoopDriver;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHive.helpers.loadDatabases;
import com.toddbodnar.simpleHive.metastore.database;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author toddbodnar
 */
public class selectTest {

    private database db;

    public selectTest() {
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
    public void testSelectVariousColumns() throws InterruptedException, IOException {
        select sel = new select("col_1 as Name,1,col_2,3 as three");
        sel.setInput(db.getTable("table one"));

        SimpleHadoopDriver.run(sel, true);

        assertEquals("1,1,2,3", sel.getOutput().getFile().readNextLine());
        Assert.assertArrayEquals(new String[]{"Name", "1", "col_2", "three"}, sel.getOutput().getColNames());
    }

    @Test
    public void testSelectAll() throws InterruptedException, IOException {
        select sel = new select("*");
        sel.setInput(db.getTable("table one"));

        SimpleHadoopDriver.run(sel, true);

        assertEquals("1,2,3", sel.getOutput().getFile().readNextLine());
        Assert.assertArrayEquals(new String[]{"col_1", "col_2", "col_3"}, sel.getOutput().getColNames());
    }
    
}
