/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.compiler;

import com.toddbodnar.simpleHive.helpers.settings;
import java.io.IOException;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.toddbodnar.simpleHadoop.distributedHadoopDriver;
import com.toddbodnar.simpleHadoop.SimpleHadoopDriver;
import com.toddbodnar.simpleHive.subQueries.join;
import com.toddbodnar.simpleHive.subQueries.query;

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
            job.setInput(getPreReqs().get(0).job.getOutput());
        
        if(job.getClass().equals(join.class) && getPreReqs().size()>1)
        {
            ((join)job).setOtherInput(getPreReqs().get(1).job.getOutput());
        }
        
        try {
            if(settings.local)
            SimpleHadoopDriver.run(job, verbose);
        else
            
            
                distributedHadoopDriver.run(job,verbose);
            } catch (InterruptedException ex) {
                Logger.getLogger(workflow.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(workflow.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(workflow.class.getName()).log(Level.SEVERE, null, ex);
            }
        
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
            result+="\n\n"+(w==null?"null":w.toString());
        return result;
    }
    
    private boolean executed;
    
    public final query job;
    
    private LinkedList<workflow> preReqs;
    
    public static boolean verbose = false;
}
