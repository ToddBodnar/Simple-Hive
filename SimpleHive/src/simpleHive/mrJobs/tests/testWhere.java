/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs.tests;

import simpleHadoop.hadoopDriver;
import simpleHive.database;
import simpleHive.mrJobs.query;
import simpleHive.mrJobs.where;

/**
 *
 * @author toddbodnar
 */
public class testWhere {
    public static void main(String args[])
    {
        database d = database.getTestDB();
        query q = new where("_col1 >= 40");
        q.setInput(d.getTable("people"));
        hadoopDriver.run(q, true);
        System.out.println(q.getResult());
    }
}
