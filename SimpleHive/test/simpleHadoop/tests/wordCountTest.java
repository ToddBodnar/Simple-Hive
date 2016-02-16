/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHadoop.tests;

import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHive.metastore.table;
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
     * Test of map method, of class wordCount.
     */
    @Test
    public void testMap() {
        System.out.println("map");
        String input = "this is a test";
        simpleContext cont = null;
        wordCount instance = new wordCount(null);
        instance.map(input, cont);
        
    }

    /**
     * Test of reduce method, of class wordCount.
     */
    @Test
    public void testReduce() {
        System.out.println("reduce");
        String key = "";
        LinkedList<Long> values = null;
        wordCount instance = null;
        instance.reduce(key, values);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of inputFormat method, of class wordCount.
     */
    @Test
    public void testInputFormat() {
        System.out.println("inputFormat");
        simpleContext cont = null;
        wordCount instance = null;
        instance.inputFormat(cont);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of test method, of class wordCount.
     */
    @Test
    public void testTest() {
        System.out.println("test");
        boolean verbose = false;
        wordCount instance = null;
        int expResult = 0;
        int result = instance.test(verbose);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of main method, of class wordCount.
     */
    @Test
    public void testMain() {
        System.out.println("main");
        String[] args = null;
        wordCount.main(args);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of numTests method, of class wordCount.
     */
    @Test
    public void testNumTests() {
        System.out.println("numTests");
        wordCount instance = null;
        int expResult = 0;
        int result = instance.numTests();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of setInput method, of class wordCount.
     */
    @Test
    public void testSetInput() {
        System.out.println("setInput");
        table in = null;
        wordCount instance = null;
        instance.setInput(in);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getInput method, of class wordCount.
     */
    @Test
    public void testGetInput() {
        System.out.println("getInput");
        wordCount instance = null;
        table expResult = null;
        table result = instance.getInput();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getOutput method, of class wordCount.
     */
    @Test
    public void testGetOutput() {
        System.out.println("getOutput");
        wordCount instance = null;
        table expResult = null;
        table result = instance.getOutput();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of setOutput method, of class wordCount.
     */
    @Test
    public void testSetOutput() {
        System.out.println("setOutput");
        table table = null;
        wordCount instance = null;
        instance.setOutput(table);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
