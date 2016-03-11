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
import com.toddbodnar.simpleHive.helpers.giveHelp;
import com.toddbodnar.simpleHive.metastore.database;
import com.toddbodnar.simpleHive.subQueries.printString;
import com.toddbodnar.simpleHive.subQueries.select;
import com.toddbodnar.simpleHive.subQueries.where;
import com.toddbodnar.simpleHive.metastore.table;
import com.toddbodnar.simpleHive.subQueries.colStats;
import com.toddbodnar.simpleHive.subQueries.join;

/**
 * Builds a Parser tree from a set of tokens
 *
 * Simplifying assumption: can just read left to right Later should make this
 * into a 'real' parser
 *
 * @author toddbodnar
 */
public class Parser {

    public static workflow parse(LinkedList<String> tokens) throws Exception {
        if (tokens.size() < 1) {
            throw new Exception("Parse exception (empty string)");
        }

        if (tokens.get(0).equalsIgnoreCase("select")) {
            LinkedList<String> partial = (LinkedList<String>) tokens.clone();
            partial.removeFirst();
            return parseSelect(partial);
        }

        if (tokens.get(0).equalsIgnoreCase("colstats")) {
            LinkedList<String> partial = (LinkedList<String>) tokens.clone();
            partial.removeFirst();
            return parseColStats(partial);
        }

        if (tokens.get(0).equalsIgnoreCase("show") && tokens.get(1).equalsIgnoreCase("tables")) {
            return new workflow(new printString(settings.currentDB.showTables()));
        }

        if (tokens.get(0).equalsIgnoreCase("describe")) {
            table t = settings.currentDB.getTable(tokens.get(1));
            if (t == null) {
                throw new Exception("Parse exception (unknown table " + tokens.get(1) + ")");
            }
            return new workflow(new printString(t.describe()));
        }
        
        if(tokens.get(0).equalsIgnoreCase("help"))
        {
            parseHelp(tokens);
            return null;
        }

        return null;

    }

    private static workflow parseSelect(LinkedList<String> partial) throws Exception {
        workflow select = null;

        //a linked list style of joins
        workflow leftJoinStart = null;
        workflow leftJoinEnd = null;

        workflow where = null;
        workflow head = null;

        Iterator<String> itr = partial.iterator();
        String next = itr.next();

        String selectArgs = "";
        while (next != null && !next.equalsIgnoreCase("from")) {
            selectArgs += next + " ";
            next = itr.next();
        }
        next = itr.next();
        String fromTableStr = next;
        table fromTable = settings.currentDB.getTable(fromTableStr);
        if (fromTable == null) {
            throw new Exception("SQL Error: cannot find table: " + ((fromTableStr == null) ? "null" : fromTableStr) + " in database " + ((settings.currentDB == null) ? "null" : settings.currentDB));
        }

        select = new workflow(new select(selectArgs));
        head = select;
        select.job.setInput(fromTable);
        if (itr.hasNext()) {
            next = itr.next();
            while (next.equalsIgnoreCase("JOIN")) {
                fromTableStr = itr.next();
                table joinTable = settings.currentDB.getTable(fromTableStr);
                if (joinTable == null) {
                    throw new Exception("SQL Error: cannot find table: " + ((fromTableStr == null) ? "null" : fromTableStr) + " in database " + ((settings.currentDB == null) ? "null" : settings.currentDB));
                }

                next = itr.next();
                if (!next.equalsIgnoreCase("ON")) {
                    throw new Exception("SQL Error: Expected keyword 'ON', found " + next);
                }
                String t1JoinCol = itr.next();
                next = itr.next();
                String t2JoinCol = itr.next();
                if (!next.equals("=")) {
                    throw new Exception("SQL Error: Expected '=' in join statement, got " + next);
                }

                int t1JoinColNum = fromTable.getColNum(t1JoinCol);
                int t2JoinColNum = joinTable.getColNum(t2JoinCol);

                if (t1JoinColNum == -1) {
                    throw new Exception("SQL Error: Could not find column '" + t1JoinCol + "' in table " + fromTable);
                }
                if (t2JoinColNum == -1) {
                    throw new Exception("SQL Error: Could not find column '" + t2JoinCol + "' in table " + joinTable);
                }

                join join = new join(t1JoinColNum, t2JoinColNum);
                join.setInput(fromTable);
                join.setOtherInput(joinTable);

                workflow joinwf = new workflow(join);
                select.addPreReq(joinwf);
                select = joinwf;

                if (itr.hasNext()) {
                    next = itr.next();
                } else {
                    next = "null";
                }
            }
            if (itr.hasNext()) {
                //next = itr.next();
                if (!next.equalsIgnoreCase("WHERE")) {
                    throw new Exception("Expected where clause, found " + next);
                }

                String remaining = "";
                while (itr.hasNext()) {
                    remaining += itr.next() + " ";
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
    private static workflow parseColStats(LinkedList<String> tokens) throws Exception {
        int tokenCount = 1;
        int columnId, groupId = -1;
        String column_name = tokens.get(0);

        String group_name = null;
        if (tokens.get(1).equalsIgnoreCase("GROUP")) {
            if (!tokens.get(2).equalsIgnoreCase("BY")) {
                throw new Exception("SQL Error: Expected keyword 'on' found " + tokens.get(2));
            }
            group_name = tokens.get(3);
            tokenCount = 4;
        }
        if (!tokens.get(tokenCount).equalsIgnoreCase("FROM")) {
            throw new Exception("SQL Error: Expected keyword 'from' found " + tokens.get(tokenCount));
        }
        tokenCount++;
        if (tokens.get(tokenCount).equalsIgnoreCase("SELECT")) {
            throw new Exception("Not Yet Implemented: Doing col stats on a generated table (using SELECT)");
        }

        table input = settings.currentDB.getTable(tokens.get(tokenCount));
        if (input == null) {
            throw new Exception("SQL Error: cannot find table: " + ((tokens.get(tokenCount) == null) ? "null" : tokens.get(tokenCount)) + " in database " + ((settings.currentDB == null) ? "null" : settings.currentDB));
        }

        columnId = input.getColNum(column_name);
        if (columnId == -1) {
            throw new Exception("SQL Error: cannot find column " + column_name);
        }

        if (group_name != null) {
            groupId = input.getColNum(group_name);
            if (groupId == -1) {
                throw new Exception("SQL Error: cannot find column " + group_name);
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
