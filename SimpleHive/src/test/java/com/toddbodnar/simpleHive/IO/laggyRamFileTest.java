/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.IO;

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
public class laggyRamFileTest {
    static laggyRamFile file;
    public laggyRamFileTest() {
    }
    
    @BeforeClass
    public static void setUp() {
        file = new laggyRamFile(50);
        file.append("testline");
        file.append("anotherline");
    }
    
    @AfterClass
    public static void tearDown() {
    }
    
    @Before
    public void setUpClass() {
    }
    
    @After
    public void tearDownClass() {
    }

    /**
     * Test of readNextLine method, of class laggyRamFile.
     */
    @Test
    public void testReadNextLine() {
        String expResult = "testline";
        String result = file.readNextLine();
        assertEquals(expResult, result);
    }

    /**
     * Test of hasNext method, of class laggyRamFile.
     */
    @Test
    public void testHasNext() {
        boolean expResult = true;
        boolean result = file.hasNext();
        assertEquals(expResult, result);
        
        file.readNextLine();
        file.readNextLine();
        assertEquals(file.hasNext(),false);
    }

    /**
     * Test of resetStream method, of class laggyRamFile.
     */
    @Test
    public void testResetStream() {
        file.readNextLine();
        file.readNextLine();
        file.readNextLine();
        file.resetStream();
        // TODO review the generated test code and remove the default call to fail.
        if(!file.hasNext())
            fail("Did not reset stream");
    }

    /**
     * Test of append method, of class laggyRamFile.
     */
    @Test
    public void testAppend() {
        String s = "";
        file.append("a third test");
        assertEquals(file.data.getLast(),"a third test");
    }

    /**
     * Test of toJson method, of class laggyRamFile.
     */
    @Test
    public void testToJson() {
        setUp();
        String expResult = "{type:laggyRamFile,lag:50.0,inner:{type:ramFile,lines:[\"testline\",\"anotherline\"]}}";
        String result = file.toJson();
        assertEquals(expResult, result);
    }
    
}
