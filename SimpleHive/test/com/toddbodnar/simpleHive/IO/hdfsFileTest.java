/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.IO;

import com.toddbodnar.simpleHive.helpers.GetConfiguration;
import com.toddbodnar.simpleHive.helpers.settings;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author toddbodnar
 */
public class hdfsFileTest {
    public static boolean nohdfs = false;
    @BeforeClass
    public static void setUp() {
        try{
            Path testFile = new Path("hdfs://localhost:8020///"+settings.hdfs_prefix+"/hdfsFileTest.csv");
            FileSystem fs =  FileSystem.get(GetConfiguration.get());
            if(fs.exists(testFile))
            {
                fs.delete(testFile, true);
            }
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(testFile)));
            out.write("hello\nworld!\n");
            out.close();
            fs.close();
        }catch(Exception ex)
        {
            System.err.println("Could not create file on HDFS: "+ex);
            nohdfs=true;
        }
    }
    /**
     * Test of toJson method, of class hdfsFile.
     */
    @Test
    public void testToJson() {
        System.out.println("toJson");
        if(nohdfs)
        {
            System.out.println("Couldn't connect to HDFS, skipping test: testToJSon");
            return;
        }
        hdfsFile instance = new hdfsFile(new Path("hdfs://localhost:8020///"+settings.hdfs_prefix+"/hdfsFileTest.csv"));
        String expResult = "{type:hdfsFile,file:hdfs://localhost:8020/"+settings.hdfs_prefix+"/hdfsFileTest.csv}";
        if(settings.hdfs_prefix.equals(""))
            expResult = "{type:hdfsFile,file:hdfs://localhost:8020/hdfsFileTest.csv}";
        String result = instance.toJson();
        assertEquals(expResult, result);
    }
    
    @Test
    public void testReadHDFS(){
        if(nohdfs)
        {
            System.out.println("Couldn't connect to HDFS, skipping test: testReadHDFS");
            return;
        }
        hdfsFile instance = new hdfsFile(new Path("hdfs://localhost:8020///"+settings.hdfs_prefix+"/hdfsFileTest.csv"));
        instance.resetStream();
        assertEquals("hello",instance.readNextLine());
    }
    
    @Test
    public void testWriteReadWriteReadHDFS(){
        if(nohdfs)
        {
            System.out.println("Couldn't connect to HDFS, skipping test: testWriteReadWriteReadHDFS");
            return;
        }
        hdfsFile instance = new hdfsFile(new Path("hdfs://localhost:8020///"+settings.hdfs_prefix+"/hdfsFileTest.csv"));
        instance.append("another");
        assertEquals("hello",instance.readNextLine());
        instance.append("line");
        
        assertEquals("hello",instance.readNextLine());
        
        instance.resetStream();
        assertEquals("hello",instance.readNextLine());
        assertEquals("world!",instance.readNextLine());
        assertEquals("another",instance.readNextLine());
        assertEquals("line",instance.readNextLine());
        assert(!instance.hasNext());
    }
    
}
