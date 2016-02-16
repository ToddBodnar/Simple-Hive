/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.IO;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author toddbodnar
 */
public class hdfsFile extends fileFile{

    public hdfsFile(Path theLocation)
    {
        location = theLocation;
        
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            in = new BufferedReader(new InputStreamReader(fs.open(location)));
            out = new BufferedWriter(new OutputStreamWriter(fs.append(location)));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
   

    @Override
    public void resetStream() {

        try {
            in.close();
            FileSystem fs = FileSystem.get(new Configuration());
            in = new BufferedReader(new InputStreamReader(fs.open(location)));
            next = in.readLine();
            out.flush();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public String getLocation() {
        return location.toString();
    }
    private Path location;
}
