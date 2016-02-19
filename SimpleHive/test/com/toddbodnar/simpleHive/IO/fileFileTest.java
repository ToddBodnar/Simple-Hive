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
public class fileFileTest {
    
    
    /**
     * Test of toJson method, of class fileFile.
     */
    @Test
    public void testToJson() {
        fileFile instance = new fileFile("/User/data/file.csv");
        String expResult = "{type:fileFile,file:/User/data/file.csv}";
        String result = instance.toJson();
        assertEquals(expResult, result);
    }
    
}
