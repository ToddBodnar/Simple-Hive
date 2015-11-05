/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package compiler;

import java.util.LinkedList;
import simpleHadoop.hadoopDriver;
import simpleHive.mrJobs.leftJoin;
import simpleHive.mrJobs.query;

/**
 * Used to execute a command
 * @author toddbodnar
 */
public class workflow {
    public workflow(query q, LinkedList<workflow> preReqs)
    {
        executed = false;
        job = q;
        this.preReqs = preReqs;
    }
    public void execute()
    {
        if(executed)
            return;
        for(workflow w:getPreReqs())
            w.execute();
        
        //do something here to combine result from preReqs
        
        if(getPreReqs().size()>0)
            job.setInput(getPreReqs().get(0).job.getResult());
        
        if(job.getClass().equals(leftJoin.class))
        {
            ((leftJoin)job).setOther(getPreReqs().get(1).job.getResult());
        }
        
        hadoopDriver.run(job, verbose);
        
        executed = true;
    }
    public LinkedList<workflow> getPreReqs()
    {
        return preReqs;
    }
    private boolean executed;
    
    private final query job;
    
    private LinkedList<workflow> preReqs;
    
    public static boolean verbose = false;
}
