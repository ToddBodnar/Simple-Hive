/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHDFS.tests;

import com.toddbodnar.simpleHive.helpers.tests;
import com.toddbodnar.simpleHive.IO.laggyRamFile;
import com.toddbodnar.simpleHive.IO.ramFile;

/**
 *
 * @author toddbodnar
 */
public class laggyRamTest extends tests{
    public ramFile makeRamFile()
    {
        ramFile r = new laggyRamFile(50);
        r.append("1234,23,1,Kirk");
        r.append("2345,45,1,Spock");
        r.append("3456,34,1,Bones");
        r.append("4353,35,2,Janeway");
        r.append("3462,45,2,Tuvok");
        r.append("246,75,3,Picard");
        r.append("2364,8,3,Data");
        return r;
    }

    public int test(boolean verbose) {
        ramFile toTest = makeRamFile();
        int numCorrect = 0;
        
        numCorrect+=tests.score(verbose,toTest.hasNext(),"Testing if at eof when not at eof");
        numCorrect+=tests.score(verbose,toTest.readNextLine().equals("1234,23,1,Kirk"),"Testing first line of input");
        
        for(int ct=0;ct<5;ct++)
            toTest.readNextLine();
        
        numCorrect+=tests.score(verbose,toTest.hasNext(),"Testing if at eof when near eof");
        
        numCorrect+=tests.score(verbose,toTest.readNextLine().equals("2364,8,3,Data"),"Testing last line of input");
        
        numCorrect+=tests.score(verbose,!toTest.hasNext(),"Testing if at eof when at eof");
        
        toTest.append("8694,20,1,Young Kirk");
        
        numCorrect+=tests.score(verbose,toTest.hasNext(),"Added data, testing if at eof when at eof");
        
        numCorrect+=tests.score(verbose,toTest.readNextLine().equals("8694,20,1,Young Kirk"),"Testing last line of input");
        
        toTest.resetStream();
        numCorrect+=tests.score(verbose,toTest.readNextLine().equals("1234,23,1,Kirk"),"Reset to begining of file, testing again");
        
        return numCorrect;
        
        }

    public int numTests() {
        return 8;
    }
    
}
