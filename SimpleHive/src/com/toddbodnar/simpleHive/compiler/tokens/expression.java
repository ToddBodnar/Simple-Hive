/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.compiler.tokens;

import com.toddbodnar.simpleHive.compiler.token;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author toddbodnar
 */
public class expression implements token{

    @Override
    public boolean isA(String string) {
        return true;
    }

    @Override
    public List<token[]> expansions() {
        LinkedList<token[]> result = new LinkedList<token[]>();
        result.add(new token[]{new select_token()});
        //result.add(new token[]{new variableList(), new from(), new leftJoin()});
        return result;
    }
    
}
