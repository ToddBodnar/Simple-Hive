/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs;

import java.util.LinkedList;
import simpleHadoop.simpleContext;
import simpleHive.table;

/**
 *
 * @author toddbodnar
 */
public class printString extends query{

    private String toPrint;
    public printString(String in)
    {
        toPrint = in;
        System.out.println(in);
    }
    @Override
    public table getResult() {
        return null;
    }

    @Override
    public void setInput(table in) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void init(simpleContext cont) {
        ;
    }

    @Override
    public void map(Object input, simpleContext cont) {
        ;
    }

    @Override
    public void reduce(Object key, LinkedList values) {
        ;
    }
    
    public table getInput() {
        return null;
    }
}
