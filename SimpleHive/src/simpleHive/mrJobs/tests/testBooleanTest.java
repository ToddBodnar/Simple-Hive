/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs.tests;

import helpers.tests;
import java.util.logging.Level;
import java.util.logging.Logger;
import simpleHDFS.ramFile;
import simpleHive.mrJobs.booleanTest;
import simpleHive.table;

/**
 *
 * @author toddbodnar
 */
public class testBooleanTest extends tests{

    @Override
    public int test(boolean verbose) {
        int score = 0;
        try {
            ramFile rf = new ramFile();
            rf.append("1\0"+"2\0hello");
            table t = new table(rf, new String[]{"testnum","testnum2","teststring"});
            if(verbose)
            {
                System.out.println("Making table:\ntestnum\ttestnum2\tteststring\n1\t2\thello\n");
            }
            
            score+=tests.score(verbose, new booleanTest("_col1 = 2").evaluate(t), "Test if testnum2 = 2");
            score+=tests.score(verbose, !new booleanTest("_col0 > 5").evaluate(t), "Test if testnum > 5");
            score+=tests.score(verbose, new booleanTest("_col2 = hello").evaluate(t), "Test if teststring = hello");
            
            score+=tests.score(verbose, !new booleanTest("_col0 = 5 AND _col1 = 2").evaluate(t), "Test if testnum = 5 AND testnum2 = 2");
            
            score+=tests.score(verbose, new booleanTest("_col0 = 5 OR _col1 = 2").evaluate(t), "Test if testnum = 5 OR testnum2 = 2");
        } catch (Exception ex) {
            Logger.getLogger(testBooleanTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return score;
        }

    @Override
    public int numTests() {
        return 5;
    }
    
}
