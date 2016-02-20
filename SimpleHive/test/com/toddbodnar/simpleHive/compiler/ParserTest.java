/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.compiler;

import com.toddbodnar.simpleHive.helpers.loadDatabases;
import com.toddbodnar.simpleHive.helpers.settings;
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
public class ParserTest {
    
    public ParserTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        settings.currentDB = loadDatabases.battleStarGalacticaGame();
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    
    /**
     * Test of main method, of class Parser.
     */
    @Test
    public void testParseValidate() throws Exception {
        Parser p = new Parser();
        assert(p.validate(lexer.lexStr("select variable from players").toArray(new String[]{})));
    }
    
    @Test
    public void testParseValidate2() throws Exception {
        Parser p = new Parser();
        assert(p.validate(lexer.lexStr("select variable,variable_2, var3 as t from players").toArray(new String[]{})));
    }
}
