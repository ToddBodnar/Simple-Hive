/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs.tests;

import com.toddbodnar.simpleHive.helpers.loadDatabases;
import com.toddbodnar.simpleHadoop.localMRDriver;
import com.toddbodnar.simpleHive.metastore.database;
import com.toddbodnar.simpleHive.subQueries.query;
import com.toddbodnar.simpleHive.subQueries.where;

/**
 *
 * @author toddbodnar
 */
public class testWhere {
    public static void main(String args[])
    {
        database d = loadDatabases.starTrek();
        query q = new where("_col1 >= 40");
        q.setInput(d.getTable("people"));
        localMRDriver.run(q, true);
        System.out.println(q.getResult());
    }
}
