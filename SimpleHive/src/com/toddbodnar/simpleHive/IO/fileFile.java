/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.IO;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A file object represented by a file on the HD
 * @author toddbodnar
 */
public class fileFile implements file{

    protected fileFile()
    {
        
    }
    public fileFile(String inFile)
    {
        this(new File(inFile));
    }
    public fileFile(File inFile)
    {
        theFile = inFile;
        try {
            in = new BufferedReader(new FileReader(theFile));
            out = new BufferedWriter(new FileWriter(theFile,true));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    @Override
    public String readNextLine() {
        String toReturn = next;
        try {
            next = in.readLine();
        } catch (IOException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        }
        return toReturn;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public void resetStream() {
        try {
            in.close();
            in = new BufferedReader(new FileReader(theFile));
            next = in.readLine();
            out.flush();
        } catch (IOException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }

    @Override
    public void append(String s) {
        try {
            out.append(s);
        } catch (IOException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private File theFile;
    protected BufferedReader in;
    protected BufferedWriter out;
    protected String next;

    @Override
    public String getLocation() {
        return theFile.toString();
    }

    @Override
    public String toJson() {
        return "{type:fileFile,file:"+theFile.toString()+"}";
    }
}
