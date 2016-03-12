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
import com.toddbodnar.simpleHive.helpers.garbage_collector;
import com.toddbodnar.simpleHive.helpers.giveHelp;
import com.toddbodnar.simpleHive.metastore.database;
import com.toddbodnar.simpleHive.subQueries.printString;
import com.toddbodnar.simpleHive.subQueries.select;
import com.toddbodnar.simpleHive.subQueries.where;
import com.toddbodnar.simpleHive.metastore.table;
import com.toddbodnar.simpleHive.subQueries.colStats;
import com.toddbodnar.simpleHive.subQueries.join;
import java.text.ParseException;

/**
 * Builds a Parser tree from a set of tokens
 *
 * Simplifying assumption: can just read left to right Later should make this
 * into a 'real' parser
 *
 * @author toddbodnar
 */
public class Parser {

    public static workflow parse(LinkedList<String> tokens) throws ParseException {
        LinkedList<String> partial = (LinkedList<String>) tokens.clone();
        if(partial.size() > 0)
            partial.removeFirst();
            
        if (tokens.size() < 1) {
            throw new ParseException("Parse exception (empty string)",0);
        }
        
        switch(tokens.get(0).toLowerCase())
        {
            case "select":
                return parseSelect(partial);
            case "colstats":
                return parseColStats(partial);
            case "show":
                if(tokens.size() == 1 || !tokens.get(1).equalsIgnoreCase("tables"))
                {
                    throw new ParseException("Parse Error: Expected 'tables' got"+(tokens.size()==1?"null":tokens.get(1)),1);
                }
                System.out.println(settings.currentDB.showTables());
                break;
            case "describe":
                if(tokens.size() == 1 )
                {
                    throw new ParseException("Parse Error: Expected table name got nothing",1);
                }
                table t = settings.currentDB.getTable(tokens.get(1));
                if (t == null) {
                    throw new ParseException("Parse exception (unknown table " + tokens.get(1) + ")",1);
                }
                System.out.println(t.describe());
                break;
            case "help":
                parseHelp(tokens);
                break;
            case "gc":
                parseGC(partial);
                break;
            default:
                throw new ParseException("Parse Error: Couldn't parse '"+tokens.getFirst()+"'!",0);
        } 
        
        System.err.println("Note: Not running any map/reduce jobs for this query");

        return null;
    }
    
    private static workflow parseGC(LinkedList<String> partial) throws ParseException
    {
        if(partial.size() == 0)
        {
            System.out.println("Running Garbage Collector...");
            int removed = garbage_collector.collect();
            System.out.println("Removed "+removed+" file(s).");
        }
        else if (partial.get(0).equalsIgnoreCase("stats"))
        {
            System.out.println(garbage_collector.stats());
            
        }
        else
        {
            throw new ParseException("Parse Error: Expected '' or 'stats', got "+partial.get(0),1);
        }
         return null;
    }

    private static workflow parseSelect(LinkedList<String> partial) throws ParseException {
        workflow select = null;

        //a linked list style of joins
        workflow leftJoinStart = null;
        workflow leftJoinEnd = null;

        workflow where = null;
        workflow head = null;

        Iterator<String> itr = partial.iterator();
        String next = itr.next();
        int itrnum = 1;
        
        String selectArgs = "";
        while (next != null && !next.equalsIgnoreCase("from")) {
            selectArgs += next + " ";
            next = itr.next();
            itrnum++;
        }
        next = itr.next();
        itrnum++;
        String fromTableStr = next;
        table fromTable = settings.currentDB.getTable(fromTableStr);
        if (fromTable == null) {
            throw new ParseException("SQL Error: cannot find table: " + ((fromTableStr == null) ? "null" : fromTableStr) + " in database " + ((settings.currentDB == null) ? "null" : settings.currentDB),itrnum);
        }

        select = new workflow(new select(selectArgs));
        head = select;
        select.job.setInput(fromTable);
        if (itr.hasNext()) {
            next = itr.next();
            itrnum++;
            while (next.equalsIgnoreCase("JOIN")) {
                fromTableStr = itr.next();
                itrnum++;
                table joinTable = settings.currentDB.getTable(fromTableStr);
                if (joinTable == null) {
                    throw new ParseException("SQL Error: cannot find table: " + ((fromTableStr == null) ? "null" : fromTableStr) + " in database " + ((settings.currentDB == null) ? "null" : settings.currentDB),itrnum);
                }

                next = itr.next();
                itrnum++;
                if (!next.equalsIgnoreCase("ON")) {
                    throw new ParseException("SQL Error: Expected keyword 'ON', found " + next,itrnum);
                }
                String t1JoinCol = itr.next();
                itrnum++;
                next = itr.next();
                String t2JoinCol = itr.next();
                itrnum++;
                if (!next.equals("=")) {
                    itrnum++;
                    throw new ParseException("SQL Error: Expected '=' in join statement, got " + next,itrnum);
                }

                int t1JoinColNum = fromTable.getColNum(t1JoinCol);
                int t2JoinColNum = joinTable.getColNum(t2JoinCol);

                if (t1JoinColNum == -1) {
                    throw new ParseException("SQL Error: Could not find column '" + t1JoinCol + "' in table " + fromTable,itrnum-2);
                }
                if (t2JoinColNum == -1) {
                    throw new ParseException("SQL Error: Could not find column '" + t2JoinCol + "' in table " + joinTable,itrnum);
                }

                join join = new join(t1JoinColNum, t2JoinColNum);
                join.setInput(fromTable);
                join.setOtherInput(joinTable);

                workflow joinwf = new workflow(join);
                select.addPreReq(joinwf);
                select = joinwf;

                if (itr.hasNext()) {
                    itrnum++;
                    next = itr.next();
                } else {
                    next = "null";
                }
            }
            if (itr.hasNext()) {
                //next = itr.next();
                if (!next.equalsIgnoreCase("WHERE")) {
                    throw new ParseException("Expected where clause, found " + next,itrnum);
                }

                String remaining = "";
                while (itr.hasNext()) {
                    remaining += itr.next() + " ";
                    itrnum++;
                }
                where = new workflow(new where(remaining));
                where.job.setInput(fromTable);
                select.addPreReq(where);
            }

        }
        System.out.println(head);
        return head;
    }

    /**
     * Parse a col stats call in the form of "colstats COL_NAME [group by
     * COL_NAME] from TABLE"
     *
     * @param tokens
     * @return
     */
    private static workflow parseColStats(LinkedList<String> tokens) throws ParseException {
        int tokenCount = 1;
        int columnId, groupId = -1;
        String column_name = tokens.get(0);

        String group_name = null;
        if (tokens.get(1).equalsIgnoreCase("GROUP")) {
            if (!tokens.get(2).equalsIgnoreCase("BY")) {
                throw new ParseException("SQL Error: Expected keyword 'on' found " + tokens.get(2),2);
            }
            group_name = tokens.get(3);
            tokenCount = 4;
        }
        if (!tokens.get(tokenCount).equalsIgnoreCase("FROM")) {
            throw new ParseException("SQL Error: Expected keyword 'from' found " + tokens.get(tokenCount),tokenCount);
        }
        tokenCount++;
        if (tokens.get(tokenCount).equalsIgnoreCase("SELECT")) {
            throw new ParseException("Not Yet Implemented: Doing col stats on a generated table (using SELECT)",tokenCount);
        }

        table input = settings.currentDB.getTable(tokens.get(tokenCount));
        if (input == null) {
            throw new ParseException("SQL Error: cannot find table: " + ((tokens.get(tokenCount) == null) ? "null" : tokens.get(tokenCount)) + " in database " + ((settings.currentDB == null) ? "null" : settings.currentDB),tokenCount);
        }

        columnId = input.getColNum(column_name);
        if (columnId == -1) {
            throw new ParseException("SQL Error: cannot find column " + column_name,tokenCount);
        }

        if (group_name != null) {
            groupId = input.getColNum(group_name);
            if (groupId == -1) {
                throw new ParseException("SQL Error: cannot find column " + group_name,tokenCount);
            }
        }

        workflow wf = new workflow(new colStats(columnId, groupId));
        wf.job.setInput(input);
        return wf;
    }

    public static void main(String args[]) throws Exception {
        settings.currentDB = loadDatabases.starTrek();
        System.out.println(parse(lexer.lexStr("select 2,name, something from people")));
        System.out.println(parse(lexer.lexStr("select 2,name, something from people where _col1 >_col4")));
    }

    private static void parseHelp(LinkedList<String> tokens) {
        if(tokens.size() == 1)
            System.out.println(giveHelp.help());
        else
            System.out.println(giveHelp.help(tokens.get(1)));
    }
}
