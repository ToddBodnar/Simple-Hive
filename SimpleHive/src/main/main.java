/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import compiler.Parser;
import compiler.lexer;
import compiler.workflow;
import helpers.settings;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import simpleHive.database;
import simpleHive.table;

/**
 *
 * @author toddbodnar
 */
public class main {
    public static void main(String args[])
    {
        settings.currentDB = database.getTestDB();
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
