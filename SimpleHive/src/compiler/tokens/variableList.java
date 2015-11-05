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
public class variableList implements token {

    @Override
    public boolean isA(String string) {
        return string.toLowerCase().startsWith("_col");
    }

    @Override
    public List<token[]> expansions() {
        LinkedList<token[]> result = new LinkedList<token[]>();
        result.add(new token[]{new comma(), new variableList()});
        result.add(new token[]{});
        result.add(new token[]{ new as(), new stringToken()});
        result.add(new token[]{ new as(), new stringToken(),new comma(), new variableList()});
        return result;
    }

    
}
