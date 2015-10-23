/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package helpers;

import simpleHDFS.tests.laggyRamTest;
import simpleHDFS.tests.ramFileTest;
import simpleHadoop.tests.wordCount;
import simpleHive.mrJobs.tests.testBooleanTest;

/**
 *
 * @author toddbodnar
 */
public class testDriver {
    public static void main(String args[])
    {
        int numTests = 0;
        int numPass = 0;
        boolean verbose = true;
        
        System.out.println("Testing code\n\n");
        for(int ct=0;ct<toTest.length;ct++)
        {
            System.out.println("Testing "+names[ct]);
            int score = toTest[ct].test(verbose);
            System.out.println("Completed "+score+"/"+toTest[ct].numTests()+"\n");
            numTests+=toTest[ct].numTests();
            numPass+=score;
            
        }
        System.out.println("Final score: "+numPass+"/"+numTests);
        if(numPass!=numTests)
            System.out.println("WARNING: SOME TESTS FAILED");
    }

    static tests[] toTest = {new ramFileTest(), new laggyRamTest(), new wordCount(null), new testBooleanTest()};
    static String[] names = {"simpleHDFS/ramFile.java","simpleHDFS/laggyRamFile.java","simpleHadoop/tests/wordCount.java", "simpleHive/mrJobs/booleanTests.java"};
}
