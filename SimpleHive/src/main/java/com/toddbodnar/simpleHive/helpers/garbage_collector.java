/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.helpers;

import com.toddbodnar.simpleHive.IO.file;
import java.util.HashSet;

/**
 *
 * @author toddbodnar
 */
public class garbage_collector {
    private static HashSet<file> auto_files = new HashSet<file>(); //set of files algorithmically created (which may not necessarily be kept over time
    
    public static void noteCreated(file theFile)
    {
        auto_files.add(theFile);
    }
    
    public static void persist(file theFile)
    {
        auto_files.remove(theFile);
    }
    
    public static int collect()
    {
        //files not removed by the gc call
        HashSet<file> new_auto_files = new HashSet<file>();
        int removed = 0;
        for(file f:auto_files)
        {
            //TODO: test if auto_files have been added to metastore
            //Which isn't actually supported yet
            //Store them in the new_auto_files if yes, otherwise delete (f.delete())
            
            f.delete();
            removed ++;
        }
        
        auto_files = new_auto_files;
        
        return removed;
    }
    
    public static String stats()
    {
        switch(auto_files.size())
        {
            case 0:
                return "Garbage collector is currently not tracking any files to be collected.";
            case 1:
                return "Garbage collector currently has 1 file that may be collected.";
            default:
                return "Garbage collector currently has "+auto_files.size()+" files that may be collected.";
        }
    }
}
