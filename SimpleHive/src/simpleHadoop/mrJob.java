/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHadoop;

import java.util.LinkedList;
import java.util.Set;

/**
 *
 * @author toddbodnar
 */
public interface mrJob <mapInType,keyType,valueType>{
    public void init(context cont);
    public void map(mapInType input, context cont);
    public void reduce(keyType key, LinkedList<valueType> values);
}
