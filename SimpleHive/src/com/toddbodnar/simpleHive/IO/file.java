/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.IO;

/**
 * Interface for a file stream
 * @author toddbodnar
 */
public interface file{

    /**
     * Returns the next line of the file
     * @return 
     */
    public String readNextLine();
    
    /**
     * Are we NOT at the end of the file
     * @return 
     */
    public boolean hasNext();
    
    /**
     * Return to the beginning of the stream
     */
    public void resetStream();
    
    public void append(String s);
    
    public String getLocation();

    /**
     * For writing the file's metadata to a file
     * @return 
     */
    public String toJson();
}
