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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

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
        
        resetStream();
    }
   

    @Override
    public void resetStream() {

        try {
            if(out!=null)
                out.close();
            writing = false;
            if(in!=null)
                in.close();
            FileSystem fs = FileSystem.get(GetConfiguration.get());
            
            if(fs.isFile(location))
            {
                LinkedList<FileStatus> file = new LinkedList<>();
                file.add(fs.getFileStatus(location));
                theFiles = file.iterator();
            }
            else
            {
                LinkedList<FileStatus> files = new LinkedList<>();
                RemoteIterator<LocatedFileStatus> fileremote = fs.listFiles(location, true);
                while(fileremote.hasNext())
                    files.add(fileremote.next());
                theFiles = files.iterator();
            }
            
            FileStatus nextFileStatus;
            do
            {
                if(!theFiles.hasNext())
                {
                    System.err.println("WARNING: File is Empty");
                    super.next=null;
                    return;
                }
                nextFileStatus = theFiles.next();
            }while(fs.isDirectory(nextFileStatus.getPath())|| nextFileStatus.getLen()==0);
                
            
            in = new BufferedReader(new InputStreamReader(fs.open(nextFileStatus.getPath())));
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
        
        String toReturn = next;
        try {
            FileSystem fs = FileSystem.get(GetConfiguration.get());
            
            next = in.readLine();
            
            if(next ==null)//at end of current file
            {
                FileStatus nextFileStatus;
                do {
                    if (!theFiles.hasNext()) {
                        super.next = null;
                        return toReturn;
                    }
                    nextFileStatus = theFiles.next();
                } while (fs.isDirectory(nextFileStatus.getPath()) || nextFileStatus.getLen() == 0);
                in = new BufferedReader(new InputStreamReader(fs.open(nextFileStatus.getPath())));
                next = in.readLine();
            }
        } catch (IOException ex) {
            Logger.getLogger(fileFile.class.getName()).log(Level.SEVERE, null, ex);
        }
        return toReturn;
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
    
    public void delete()
    {
        try {
            FileSystem fs = FileSystem.get(GetConfiguration.get());

            fs.delete(location, true);
            
        } catch (IOException ex) {
            Logger.getLogger(hdfsFile.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private Iterator<FileStatus> theFiles;
    private boolean writing = false;
    private Path location;
}
