/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.IO;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A ram file that can take up to a second to process, to simulate network lag/etc
 * @author toddbodnar
 */
public class laggyRamFile extends ramFile{
    double maxLagMiliSeconds;
    public laggyRamFile(double maxLagMiliSeconds)
    {
        super();
        this.maxLagMiliSeconds = maxLagMiliSeconds;
    }
    
    private void lag()
    {
        try {
            Thread.sleep((long) (Math.random()*maxLagMiliSeconds));
        } catch (InterruptedException ex) {
            Logger.getLogger(laggyRamFile.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public laggyRamFile()
    {
        this(1000);
    }
    
    public String readNextLine()
    {
        lag();
        return super.readNextLine();
    }
    public boolean hasNext()
    {
        lag();
        return super.hasNext();
    }
    
    public void resetStream()
    {
        lag();
        super.resetStream();
    }
    
    public void append(String s)
    {
        lag();
        super.append(s);
    }
}
