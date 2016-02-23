/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs.tests;

import com.toddbodnar.simpleHive.helpers.loadDatabases;
import com.toddbodnar.simpleHadoop.SimpleHadoopDriver;
import com.toddbodnar.simpleHive.metastore.database;
import com.toddbodnar.simpleHive.subQueries.query;
import com.toddbodnar.simpleHive.subQueries.where;
import java.io.IOException;

/**
 *
 * @author toddbodnar
 */
public class testWhere {
    public static void main(String args[]) throws IOException, InterruptedException
    {
        database d = loadDatabases.starTrek();
        query q = new where("age >= 40");
        q.setInput(d.getTable("people"));
        SimpleHadoopDriver.run(q, true);
        System.out.println(q.getOutput().print());
    }
}
