/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs.tests;

import com.toddbodnar.simpleHive.compiler.lexer;
import com.toddbodnar.simpleHive.compiler.Parser;
import com.toddbodnar.simpleHive.compiler.workflow;
import com.toddbodnar.simpleHive.helpers.loadDatabases;
import com.toddbodnar.simpleHive.helpers.settings;
import com.toddbodnar.simpleHadoop.SimpleHadoopDriver;
import com.toddbodnar.simpleHive.metastore.database;
import com.toddbodnar.simpleHive.subQueries.colStats;
import com.toddbodnar.simpleHive.subQueries.leftJoin;
import com.toddbodnar.simpleHive.subQueries.query;
import com.toddbodnar.simpleHive.subQueries.select;
import com.toddbodnar.simpleHive.subQueries.where;
import com.toddbodnar.simpleHive.metastore.table;

/**
 *
 * @author toddbodnar
 */
public class playground {
    public static void main(String args[]) throws Exception
    {
        database db = loadDatabases.starTrek();
        
        settings.currentDB = db;
        
        query stats = new colStats(1,2);//summarize age group by ship number
        stats.setInput(db.getTable("people"));
        SimpleHadoopDriver.run(stats, true);
        System.out.println("\n"+stats.getOutput().print());
        /*
        System.out.println(db.getTable("people").toJson("people"));
        query aWhere = new where("_col1 >= 40");
        aWhere.setInput(db.getTable("people"));
        
        SimpleHadoopDriver.run(aWhere, true);
        query aSelect = new select("name as Name,1 as one,age,3 as three");
        aSelect.setInput(aWhere.getResult());
        //System.out.println(aWhere.getResult());
        SimpleHadoopDriver.run(aSelect, true);
        System.out.println(aSelect.getResult().print());
        
        query join = new leftJoin(db.getTable("ships"),2,0);
        join.setInput(db.getTable("people"));
        SimpleHadoopDriver.run(join, true);
        
        query Select = new select("name as Name,shipname as Ship,age");
        Select.setInput(join.getResult());
        SimpleHadoopDriver.run(Select, true);
        System.out.println("\n"+Select.getResult().print());
        
        
        workflow wf = Parser.parse(lexer.lexStr("select    *    from people"));
        System.out.println(wf);
        wf.execute();
        System.out.println(wf.job.getResult().print());
        
        Parser.parse(lexer.lexStr("show \t\tTaBles"));
        
        Parser.parse(lexer.lexStr("describe people"));
        
        query stats = new colStats(1,2);//summarize age group by ship number
        stats.setInput(db.getTable("people"));
        SimpleHadoopDriver.run(stats, true);
        System.out.println("\n"+stats.getResult().print());*/
    }
}
