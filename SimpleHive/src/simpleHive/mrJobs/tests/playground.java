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
import simpleHive.table;

/**
 *
 * @author toddbodnar
 */
public class playground {
    public static void main(String args[])
    {
        database d = database.getTestDB();
        query aWhere = new where("_col1 >= 40");
        aWhere.setInput(d.getTable("people"));
        
        hadoopDriver.run(aWhere, true);
        query aSelect = new select("_col3 as Name,1 as one,_col1,3 as three");
        aSelect.setInput(aWhere.getResult());
        //System.out.println(aWhere.getResult());
        hadoopDriver.run(aSelect, true);
        System.out.println(aSelect.getResult());
        
    }
}
