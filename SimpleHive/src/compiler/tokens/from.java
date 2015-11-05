/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package compiler.tokens;

import compiler.token;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author toddbodnar
 */
public class from implements token {

    @Override
    public boolean isA(String string) {
        return string.equalsIgnoreCase("from");
    }

    @Override
    public List<token[]> expansions() {
        LinkedList<token[]> result = new LinkedList<token[]>();
        
        result.add(new token[]{}); //from doesn't have any arguments (the table is part of select)
        return result;
    }
    
}
