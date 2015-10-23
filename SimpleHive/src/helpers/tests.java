/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package helpers;

/**
 *
 * @author toddbodnar
 */
public abstract class tests {
    /**
     * Run through all tests
     * @param verbose should we output info or just do the tests?
     * @return the number of correct responses to the test
     */
    public abstract int test(boolean verbose);
    
    /**
     * The number of tests scored
     * @return 
     */
    public abstract int numTests();
    
    /**
     * Helper function for formatting/etc.
     * @param verbose Should we output or just score?
     * @param theTest did the test pass?
     * @param description How to describe the test
     * @return 1 if passed, 0 otherwise
     */
    public static int score(boolean verbose,boolean theTest,String description)
    {
        if(!verbose)
            return theTest?1:0;
        System.out.println("\t"+description+": "+(theTest?"pass":"FAIL"));
        return theTest?1:0;
    }
}
