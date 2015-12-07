/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package compiler;

import java.util.LinkedList;
import simpleHadoop.localMRDriver;
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
    public workflow(query q)
    {
        this(q,new LinkedList<workflow>());
    }
    public void addPreReq(workflow w)
    {
        //more arguements invalidate any previous work done
        executed = false;
        preReqs.add(w);
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
        
        localMRDriver.run(job, verbose);
        
        executed = true;
    }
    
    public LinkedList<workflow> getPreReqs()
    {
        return preReqs;
    }
    
    public String toString()
    {
        String result = "Workflow element\nUsing job:\n"+job+"\n\nWith "+preReqs.size()+" job(s) that must be completed first:";
        for(workflow w:preReqs)
            result+="\n\n"+w.toString();
        return result;
    }
    
    private boolean executed;
    
    public final query job;
    
    private LinkedList<workflow> preReqs;
    
    public static boolean verbose = false;
}
