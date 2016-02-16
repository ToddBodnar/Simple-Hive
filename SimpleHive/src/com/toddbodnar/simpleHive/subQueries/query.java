/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import java.util.LinkedList;
import com.toddbodnar.simpleHive.IO.file;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHadoop.MapReduceJob;
import com.toddbodnar.simpleHive.metastore.table;

/**
 *
 * @author toddbodnar
 */
public abstract class query extends MapReduceJob{
    
    public abstract table getResult();
}
