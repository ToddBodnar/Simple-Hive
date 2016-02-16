/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.compiler;

import com.toddbodnar.simpleHive.compiler.tokens.variableList;
import com.toddbodnar.simpleHive.compiler.tokens.select;
import com.toddbodnar.simpleHive.compiler.tokens.comma;
import com.toddbodnar.simpleHive.compiler.tokens.from;
import com.toddbodnar.simpleHive.compiler.tokens.table;
import com.toddbodnar.simpleHive.helpers.loadDatabases;
import com.toddbodnar.simpleHive.helpers.settings;
import java.util.LinkedList;
import com.toddbodnar.simpleHive.metastore.database;

/**
 *
 * @author toddbodnar
 */
public class lexer {
    public static LinkedList<String> lexStr(String in) throws Exception
    {
        LinkedList<String> tokenized = new LinkedList<String>();
        
        String partial="";
        for(char c:in.toCharArray())
        {
            switch(c)
            {
                case ' ':
                case '\n':
                case '\r':
                case '\t':
                    if(partial.equals(""))
                        continue;
                    tokenized.add(partial);
                    partial = "";
                    break;
                case ',':
                case '=':
                case '<':
                case '>':
                    if(!partial.equals(""))
                        tokenized.add(partial);
                    tokenized.add(c+"");
                    partial="";
                    break;
                default:
                    partial+=c;
                    
            }
        }
        
        if(!partial.equals(""))
            tokenized.add(partial);
        
        return tokenized;
        
    }
    public static LinkedList<token> lex(String input) throws Exception
    {
        LinkedList<String> tokenized = lexStr(input);
        LinkedList<token> result = new LinkedList<token>();
        
        for(String s:tokenized)
        {
            token next = null;
            for(token t:allTokens)
            {
                if(t.isA(s))
                {
                    next = t;
                    break;
                }
            }
            if(next==null)
            {
                throw new Exception("Cannot determine type of "+s);
            }
            result.add(next);
        }
        
        
        return result;
    }
    
    public static token[] allTokens = new token[]{new select(),new comma(),new from(),new variableList(),new table()};
    
    public static void main(String args[]) throws Exception
    {
        settings.currentDB = loadDatabases.starTrek();
        //lex("select 3, 5, z from x;");
        lex("select _col1  , _col2        from      people");
    }
}
