/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive;

import com.toddbodnar.simpleHive.compiler.Parser;
import com.toddbodnar.simpleHive.compiler.lexer;
import com.toddbodnar.simpleHive.compiler.workflow;
import com.toddbodnar.simpleHive.helpers.loadDatabases;
import com.toddbodnar.simpleHive.helpers.settings;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.toddbodnar.simpleHive.metastore.database;
import com.toddbodnar.simpleHive.metastore.table;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author toddbodnar
 */
public class main {
    private static int contains(String args[], String val)
    {
        for(int ct=0;ct<args.length;ct++)
            if(args[ct].equals(val))
                return ct;
        return -1;
    }
    public static void init(String args[]) {
        if (contains(args, "--help") >= 0 || contains(args, "-h") >= 0) {
            System.out.println("TODO: Add help dialog. (sorry!)");
            System.exit(0);
        }

        System.out.println("Hello and welcome to Simple Hive (todo: replace intro)");

        if (contains(args, "--local") < 0) {
            System.out.println("Trying to connect to default hadoop system");
            try {
                Configuration conf = new Configuration();//TODO: test this on hadoop instance
                settings.conf = conf;
                settings.local = false;
                System.out.println("Connection successful!");
                System.out.println(settings.conf);
            } catch (Error ex) {
                System.out.println("Unable to connect, continuing on local mode.");
                settings.local = true;
            }
        }
        settings.currentDB = loadDatabases.starTrek();

        System.out.println("Using database metastore: " + settings.currentDB.toString());
    }
    public static void main(String args[])
    {
        init(args);
        
        Scanner in = new Scanner(System.in);
        in.useDelimiter("[\n\r;]+");
        while(in.hasNext())
        {
            String input = in.next();
            if(input.length()==0)
                continue;
            if(input.equalsIgnoreCase("QUIT") || input.equalsIgnoreCase("EXIT"))
            {
                break;
            }
            try {
                workflow wf = Parser.parse(lexer.lexStr(input));
                if(wf!=null)
                {
                    wf.execute();
                    table result = wf.job.getOutput();
                    if(result!=null)
                        System.out.println(result.print());
                }
            } catch (Exception ex) {
                Logger.getLogger(main.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
        System.out.println("Exiting");
    }
}
