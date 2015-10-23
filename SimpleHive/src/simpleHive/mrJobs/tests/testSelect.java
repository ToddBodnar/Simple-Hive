/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs.tests;

import simpleHadoop.hadoopDriver;
import simpleHive.database;
import simpleHive.mrJobs.query;
import simpleHive.mrJobs.select;
import simpleHive.mrJobs.where;

/**
 *
 * @author toddbodnar
 */
public class testSelect {
    public static void main(String args[])
    {
        database d = database.getTestDB();
        query q = new select("_col3 as Name,1 as one,_col1,3 as three");
        q.setInput(d.getTable("people"));
        hadoopDriver.run(q, true);
        System.out.println(q.getResult());
    }
}
