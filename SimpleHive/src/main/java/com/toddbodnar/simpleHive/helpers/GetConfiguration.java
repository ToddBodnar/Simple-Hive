/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author toddbodnar
 */
public class GetConfiguration {
    public static Configuration get()
    {
        Configuration c = new Configuration();
        for(String file:settings.config_files_xml)
        {
            c.addResource(new Path(file));
        }
        return c;
    }
}
