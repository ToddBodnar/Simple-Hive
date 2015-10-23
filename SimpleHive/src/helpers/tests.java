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
    public abstract int test(boolean verbose);
    public abstract int numTests();
    public static int score(boolean verbose,boolean theTest,String description)
    {
        if(!verbose)
            return theTest?1:0;
        System.out.println("\t"+description+": "+(theTest?"pass":"FAIL"));
        return theTest?1:0;
    }
}
