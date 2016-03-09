/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.helpers;

/**
 *
 * @author toddbodnar
 */
public class giveHelp {
    /**
     * What do say when the user uses the -h or --help flags in the command line
     */
    public static String usedHelpFlag()
    {
        String result = ""+
                " Simple Hive v "+settings.version+"\n"+
                " ----------------------\n"+
                "\n"+
                "Usage\n"+
                "\n"+
                "java -jar simpleHive.jar [-D dbname] [-h] [--help] [--local]\n"+
                "\n"+
                "Element      Use\n"+
                "-------      ---\n"+
                "-D dbname    Use database 'dbname' instead of the default database\n"+
                "\n"+
                "-h           Displays this information and exits\n"+
                "--help       \n"+
                "\n"+
                "--local      Do not try to connect to a Hadoop cluster";
        return result;
    }
}
