/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.compiler;

import com.toddbodnar.simpleHive.helpers.loadDatabases;
import com.toddbodnar.simpleHive.helpers.settings;
import java.util.Iterator;
import java.util.LinkedList;
import com.toddbodnar.simpleHadoop.distributedHadoopDriver;
import com.toddbodnar.simpleHive.compiler.tokens.select_token;
import simpleHadoop.tests.wordCount;
import com.toddbodnar.simpleHive.metastore.database;
import com.toddbodnar.simpleHive.subQueries.printString;
import com.toddbodnar.simpleHive.subQueries.select;
import com.toddbodnar.simpleHive.subQueries.where;
import com.toddbodnar.simpleHive.metastore.table;
import java.text.ParseException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Builds a Parser tree from a set of tokens
 
 Simplifying assumption: can just read left to right
 Later should make this into a 'real' parser
 * @author toddbodnar
 */
public class Parser {
    public static workflow parse(LinkedList<String> tokens) throws Exception
    {
        if(tokens.size()<1)
            throw new Exception("Parse exception (empty string)");
        
        if(tokens.get(0).equalsIgnoreCase("select"))
        {
            LinkedList<String> partial = (LinkedList<String>) tokens.clone();
            partial.removeFirst();
            return parseSelect(partial);
        }
        
        if(tokens.get(0).equalsIgnoreCase("show") && tokens.get(1).equalsIgnoreCase("tables"))
        {
            return new workflow(new printString(settings.currentDB.showTables()));
        }
        
        if(tokens.get(0).equalsIgnoreCase("describe"))
        {
            table t = settings.currentDB.getTable(tokens.get(1));
            if(t==null)
                throw new Exception("Parse exception (unknown table "+tokens.get(1)+")");
            return new workflow(new printString(t.describe()));
        }
        
        
        return null;
        
    }

    private static workflow parseSelect(LinkedList<String> partial) throws Exception {
        workflow select = null;
        
        //a linked list style of joins
        workflow leftJoinStart = null;
        workflow leftJoinEnd = null;
        
        workflow where = null;
        
        
        
        Iterator<String> itr = partial.iterator();
        String next = itr.next();
        
        String selectArgs = "";
        while(next!=null && !next.equalsIgnoreCase("from"))
        {
            selectArgs+=next+" ";
            next = itr.next();
        }
        next = itr.next();
        String fromTableStr = next;
        table fromTable = settings.currentDB.getTable(fromTableStr);
        if(fromTable==null)
        {
            throw new Exception("SQL Error: cannot find table: "+((fromTableStr==null)?"null":fromTableStr)+" in database "+((settings.currentDB==null)?"null":settings.currentDB));
        }
        
        select = new workflow(new select(selectArgs));
        
        if(itr.hasNext())
        {
            next = itr.next();
            while(next.equalsIgnoreCase("LEFT"))
            {
                //while left joining tables
                //left join table on x=y
            }
            if(itr.hasNext())
            {
                //next = itr.next();
                if(!next.equalsIgnoreCase("WHERE"))
                    throw new Exception("Expected where clause, found "+next);
                
                String remaining = "";
                while(itr.hasNext())
                {
                    remaining+=itr.next()+" ";
                }
                where = new workflow(new where(remaining));
                where.job.setInput(fromTable);
            }
            
            select.addPreReq(where);
            
        }
        else
        {
            select.job.setInput(fromTable);
        }
        return select;
    }
    
    public static void main(String args[]) throws Exception
    {
        settings.currentDB = loadDatabases.starTrek();
        System.out.println(parse(lexer.lexStr("select 2,name, something from people")));
        System.out.println(parse(lexer.lexStr("select 2,name, something from people where _col1 >_col4")));
    }
    
    private boolean match(token expected)
    {
        if(! expected.isA(tokens[token_itr]))
        {
            return false;
            //throw new ParseException("Expected "+expected+" got "+tokens[token_itr],token_itr);
        }
        return true;
    }
    
    public boolean validate(String tokens[])
    {
        token_itr = 0;
        this.tokens = tokens;
 
            if(match(new select_token()))
            {
                return validate(new select_token());
            }
            return false;
        
    }
    
    private boolean validate( token current)
    {
        if(current.expansions().isEmpty())
            return validate(current);
        
        System.out.println("testing "+current.getClass().getSimpleName()+" at space "+token_itr);
        
        for(token[] paths:current.expansions())
        {
            boolean match = true;
            int current_token = token_itr;
            for(token t:paths)
            {
                match = match &(match(t) && validate(t));
                token_itr++;
                    
            }
            token_itr = current_token;
            if(match)
            {
                System.out.print("found expansion:");
                
                
            }
            else
                System.out.print("no expansion for");
            for(token t:paths)
                    System.out.print(" "+t.getClass().getSimpleName());
                System.out.println("based on "+current.getClass().getSimpleName());
                
            if(match)
                return true;
        }
        return false;
    }
    
    String next = null;
    String []tokens = null;
    int token_itr = 0;
}
