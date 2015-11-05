/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package compiler;

import compiler.tokens.*;
import helpers.settings;
import java.util.LinkedList;
import simpleHive.database;

/**
 *
 * @author toddbodnar
 */
public class lexer {
    public static LinkedList<token> lex(String in) throws Exception
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
                    if(!partial.equals(""))
                        tokenized.add(partial);
                    tokenized.add(",");
                    partial="";
                    break;
                default:
                    partial+=c;
                    
            }
        }
        
        if(!partial.equals(""))
            tokenized.add(partial);
        
        for(String s:tokenized)
            System.out.println(s);
        
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
        settings.currentDB = database.getTestDB();
        //lex("select 3, 5, z from x;");
        lex("select _col1  , _col2        from      people");
    }
}
