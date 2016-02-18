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
    public static void init(String args[])
    {
        System.out.println("Hello and welcome to Simple Hive (todo: replace intro)");
        System.out.println("Trying to connect to default hadoop system");
        try
        {
            Configuration conf = new Configuration();//TODO: test this on hadoop instance
            settings.conf = conf;
            settings.local = false;
            System.out.println("Connection successful!");
            System.out.println(settings.conf);
        }
        catch(Error ex)
        {
            System.out.println("Unable to connect, continuing on local mode.");
            settings.local = true;
        }
        
        settings.currentDB = loadDatabases.battleStarGalacticaGame();
        System.out.println("Using database metastore: "+settings.currentDB.toString());
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
            try {
                workflow wf = Parser.parse(lexer.lexStr(input));
                if(wf!=null)
                {
                    wf.execute();
                    table result = wf.job.getResult();
                    if(result!=null)
                        System.out.println(result.print());
                }
            } catch (Exception ex) {
                Logger.getLogger(main.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
    }
}
