/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHadoop.tests;

import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import java.util.LinkedList;
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
public class wordCountTest {
    
    public wordCountTest() {
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
     * Test wordcount on local hadoop driver
     */
    @Test
    public void testMap() throws IOException, InterruptedException {
        assertEquals(new wordCount().test(false),3);
    }
    
}
