/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHDFS;

import java.util.LinkedList;

/**
 * A 'file' stored in ram
 * @author toddbodnar
 */
public class ramFile implements file{
    LinkedList<String> data;
    
    //todo: real LL iterator
    int iterator;
    
    public ramFile()
    {
        data = new LinkedList<String>();
        iterator = 0;
    }
    @Override
    public String readNextLine() {
        if(!hasNext())
        {
            return null;
        }
        String result = data.get(iterator);
        iterator++;
        return result;
    }

    @Override
    public void resetStream() {
        iterator = 0;
    }

    @Override
    public void append(String s) {
        data.add(s);
    }

    @Override
    public boolean hasNext() {
        return iterator<data.size();
    }
    
}
