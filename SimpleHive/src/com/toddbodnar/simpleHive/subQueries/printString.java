/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import java.util.LinkedList;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHive.metastore.table;

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

    @Override
    public table getOutput() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setOutput(table table) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
