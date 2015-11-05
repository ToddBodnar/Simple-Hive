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
public class select implements token{

    @Override
    public boolean isA(String string) {
        return string.equalsIgnoreCase("select");
    }

    @Override
    public List<token[]> expansions() {
        LinkedList<token[]> result = new LinkedList<token[]>();
        result.add(new token[]{new variableList(), new from(), new table()});
        result.add(new token[]{new variableList(), new from(), new leftJoin()});
        return result;
    }

    
}
