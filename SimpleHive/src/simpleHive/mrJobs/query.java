/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs;

import java.util.LinkedList;
import simpleHDFS.file;
import simpleHadoop.simpleContext;
import simpleHadoop.mrJob;
import simpleHive.table;

/**
 *
 * @author toddbodnar
 */
public abstract class query extends mrJob{
    
    public abstract table getResult();
}
