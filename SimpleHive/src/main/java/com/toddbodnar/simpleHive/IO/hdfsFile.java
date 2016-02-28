/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.IO;

import com.toddbodnar.simpleHive.helpers.GetConfiguration;
import com.toddbodnar.simpleHive.helpers.settings;
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

    public static hdfsFile transferToHDFS(file f) throws IOException
    {
        if(f.getClass().equals(hdfsFile.class))//if the file to be put on hdfs is already on hdfs
            return (hdfsFile)f;                //just return the file
        
        FileSystem fs = FileSystem.get(GetConfiguration.get());
        Path theFile;
        do
        {
            theFile = new Path(settings.hdfs_prefix+"/LOCAL_TABLE_"+System.currentTimeMillis()+"_"+Math.round(Math.random()*10000));
        }while(fs.exists(theFile));
        
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(theFile)));
        
        f.resetStream();
        while(f.hasNext())
        {
            out.write(f.readNextLine()+"\n");
        }
        out.close();
        fs.close();
        return new hdfsFile(theFile);
    }
    
    public hdfsFile(Path theLocation)
    {
        location = theLocation;
        
        try {
            FileSystem fs = FileSystem.get(GetConfiguration.get());
            in = new BufferedReader(new InputStreamReader(fs.open(location)));
            //out = new BufferedWriter(new OutputStreamWriter(fs.append(location)));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
   

    @Override
    public void resetStream() {

        try {
            if(out!=null)
                out.close();
            writing = false;
            in.close();
            FileSystem fs = FileSystem.get(GetConfiguration.get());
            in = new BufferedReader(new InputStreamReader(fs.open(location)));
            next = in.readLine();
            
            //out.flush();
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
    
    public String toJson() {
        return "{type:hdfsFile,file:"+location.toString()+"}";
    }
    
    public String readNextLine()
    {
        if(writing)
            resetStream();
        
        return super.readNextLine();
    }
    
    public void append(String line)
    {
        try {
            if(!writing)
            {
            in.close();
            FileSystem fs = FileSystem.get(GetConfiguration.get());
            out = new BufferedWriter(new OutputStreamWriter(fs.append(location)));
            writing = true;
            }
            out.write(line+"\n");
        } catch (IOException ex) {
            Logger.getLogger(hdfsFile.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public Path getPath()
    {
        return location;
    }
    
    private boolean writing = false;
    private Path location;
}
