/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHDFS;

/**
 *
 * @author toddbodnar
 */
public interface file {

    public String readNextLine();
    
    public boolean hasNext();
    
    public void resetStream();
    
    public void append(String s);
}
