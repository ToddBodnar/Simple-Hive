/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import java.util.LinkedList;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHive.metastore.table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author toddbodnar
 */
public class printString extends query<Object,Object>{

    private String toPrint;
    public printString(String in)
    {
        toPrint = in;
        System.out.println(in);
        super.setInput(null);
    }

    @Override
    public Mapper getMapper() {
        return new Mapper();    }

    @Override
    public Reducer getReducer() {
        return new Reducer();    }

    @Override
    public Class getKeyType() {
        return Object.class;
    }

    @Override
    public Class getValueType() {
        return Object.class;
    }

    @Override
    public void writeConfig(Configuration conf) {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    

   
}
