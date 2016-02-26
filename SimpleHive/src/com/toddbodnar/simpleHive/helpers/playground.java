package com.toddbodnar.simpleHive.helpers;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


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
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author toddbodnar
 */
public class playground {
    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("test", "hi");
        System.out.println(conf.get("test"));
        database db = loadDatabases.starTrek();
        
        settings.currentDB = db;
        System.out.println("Select * from ships");
        
        query simpleSelect = new select("*");
        simpleSelect.setInput(db.getTable("ships"));
        SimpleHadoopDriver.run(simpleSelect, true);
        System.out.println(simpleSelect.getOutput().print());
        
        /*System.out.println("colstats on people group by ship number");
        query stats = new colStats(1,2);//summarize age group by ship number
        stats.setInput(db.getTable("people"));
        SimpleHadoopDriver.run(stats, true);
        System.out.println("\n"+stats.getOutput().print());
        
        System.out.println("Select * from people where age >=40");
        query aWhere = new where("_col1 >= 40");
        aWhere.setInput(db.getTable("people"));
        
        SimpleHadoopDriver.run(aWhere, true);
        System.out.println(aWhere.getOutput().print());
        
        System.out.println("Select name as Name,1 as one,age,3 as three from people where age >= 40");
        query aSelect = new select("name as Name,1 as one,age,3 as three");
        aSelect.setInput(aWhere.getOutput());
        //System.out.println(aWhere.getResult());
        SimpleHadoopDriver.run(aSelect, true);
        System.out.println(aSelect.getOutput().print());
        
        System.out.println("select * from people left join ships on ship_id = id");
        leftJoin join = new leftJoin(0,2);
        join.setInput(db.getTable("ships"));
        join.setOtherInput(db.getTable("people"));
        SimpleHadoopDriver.run(join, true);
        System.out.println(join.getOutput().print());
        
        System.out.println("\n\nselect name as Name,shipname as Ship,age from people left join ships on ship_id = id");
        query Select = new select("name as Name,shipname as Ship,age");
        Select.setInput(join.getOutput());
        SimpleHadoopDriver.run(Select, true);
        System.out.println("\n"+Select.getOutput().print());
        
        
        workflow wf = Parser.parse(lexer.lexStr("select    *    from people"));
        System.out.println(wf);
        wf.execute();
        System.out.println(wf.job.getOutput().print());
        
        Parser.parse(lexer.lexStr("show \t\tTaBles"));
        
        Parser.parse(lexer.lexStr("describe people"));
        */
    }
}
