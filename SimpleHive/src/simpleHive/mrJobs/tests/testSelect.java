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
import com.toddbodnar.simpleHive.subQueries.select;
import com.toddbodnar.simpleHive.subQueries.where;

/**
 *
 * @author toddbodnar
 */
public class testSelect {
    public static void main(String args[])
    {
        database d = loadDatabases.starTrek();
        query q = new select("_col3 as Name,1 as one,_col1,3 as three");
        q.setInput(d.getTable("people"));
        localMRDriver.run(q, true);
        System.out.println(q.getResult());
    }
}
