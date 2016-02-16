/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.compiler;

import java.util.List;

/**
 *
 * @author toddbodnar
 */
public interface token {
    /**
     * Is the given string a description of this token. i.e. map "selEct","select","SELECT" to true for the select token
     * @param string
     * @return 
     */
    public boolean isA(String string);
    
    /**
     * What this token needs to be valid syntactically. i.e. select requires a list of variables, a from, and a table (in that order)
     * @return 
     */
    public List<token[]> expansions();
}
