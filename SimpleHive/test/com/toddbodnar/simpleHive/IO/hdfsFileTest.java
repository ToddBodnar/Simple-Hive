/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.IO;

import org.apache.hadoop.fs.Path;
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
public class hdfsFileTest {
    
    
    /**
     * Test of toJson method, of class hdfsFile.
     */
    @Test
    public void testToJson() {
        System.out.println("toJson");
        hdfsFile instance = new hdfsFile(new Path("http://java.sun.com/j2se/1.3/"));
        String expResult = "{type:hdfsFile,location:\"datalocation\"}";
        String result = instance.toJson();
        assertEquals(expResult, result);
    }
    
}
