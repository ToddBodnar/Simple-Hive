/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.helpers;

import com.toddbodnar.simpleHive.metastore.database;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author toddbodnar
 */
public class settings {
    public static database currentDB = null;
    public static String currentDBName = "default";
    public static boolean local = true;
    public static Configuration conf = null;
}
