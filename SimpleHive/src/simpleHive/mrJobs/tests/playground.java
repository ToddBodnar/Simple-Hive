/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs.tests;

import compiler.lexer;
import compiler.Parser;
import compiler.workflow;
import helpers.loadDatabases;
import helpers.settings;
import simpleHadoop.localMRDriver;
import simpleHive.database;
import simpleHive.mrJobs.colStats;
import simpleHive.mrJobs.leftJoin;
import simpleHive.mrJobs.query;
import simpleHive.mrJobs.select;
import simpleHive.mrJobs.where;
import simpleHive.table;

/**
 *
 * @author toddbodnar
 */
public class playground {
    public static void main(String args[]) throws Exception
    {
        database db = loadDatabases.starTrek();
        
        settings.currentDB = db;
        
        query aWhere = new where("_col1 >= 40");
        aWhere.setInput(db.getTable("people"));
        
        localMRDriver.run(aWhere, true);
        query aSelect = new select("name as Name,1 as one,age,3 as three");
        aSelect.setInput(aWhere.getResult());
        //System.out.println(aWhere.getResult());
        localMRDriver.run(aSelect, true);
        System.out.println(aSelect.getResult().print());
        
        query join = new leftJoin(db.getTable("ships"),2,0);
        join.setInput(db.getTable("people"));
        localMRDriver.run(join, true);
        
        query Select = new select("name as Name,shipname as Ship,age");
        Select.setInput(join.getResult());
        localMRDriver.run(Select, true);
        System.out.println("\n"+Select.getResult().print());
        
        
        workflow wf = Parser.parse(lexer.lexStr("select    *    from people"));
        System.out.println(wf);
        wf.execute();
        System.out.println(wf.job.getResult().print());
        
        Parser.parse(lexer.lexStr("show \t\tTaBles"));
        
        Parser.parse(lexer.lexStr("describe people"));
        
        query stats = new colStats(1,2);//summarize age group by ship number
        stats.setInput(db.getTable("people"));
        localMRDriver.run(stats, true);
        System.out.println("\n"+stats.getResult().print());
    }
}
