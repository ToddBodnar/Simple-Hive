/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.helpers;

import java.util.HashMap;

/**
 *
 * @author toddbodnar
 */
public class giveHelp {
    private static HashMap<String,String> contents;
    
    static
    {
        contents = new HashMap<>();
        String select = ""+
                " select {columns,*} from table\n"+
                "\n"+
                " Filters a 'table' with a given set of columns or '*' for all columns.\n"+
                " Columns can be either a column_name or a static value. Each column name can be renamed by using 'previous_name as new_name'. Multiple columns supported by comma delimitation.\n"+
                "\n"+
                " Example usage:\n"+
                " --------------\n"+
                " select * from people\n"+
                " select age, ship as shipId, 3 as number3 from people";
        
        String join = ""+
                " select * from table1 join table2 on table1_key = table2_key\n"+
                "\n"+
                " Inner join of two tables based on matching keys.\n"+
                //" Columns can be either a column_name or a static value. Each column name can be renamed by using 'previous_name as new_name'. Multiple columns supported by comma delimitation.\n"+
                "\n"+
                " Example usage:\n"+
                " --------------\n"+
                " select * from people join ships on ship = shipID";
        
        String where = ""+
                " where [boolean_expression]+\n"+
                "\n"+
                " Filters a 'table' based on a boolean formula.\n"+
                " Supports '<','<=','=','=>','>','!=' for comparison and 'or' and 'and' for chaining expressions.\n"+
                "\n"+
                " Example usage:\n"+
                " --------------\n"+
                " select * from people where ship = 3";
        
        String colstats = ""+
                " colstats column [group by groupcolumn] from table\n"+
                "\n"+
                " Generates summary statistics for a 'column', optionally grouped by 'groupcolumn'.\n"+
                "\n"+
                " Example usage:\n"+
                " --------------\n"+
                " colstats age group by ship from people";
        
        
        
        String help = ""+
                " help [command]\n"+
                "\n"+
                " Gives a general help page, or a help page for 'command'.\n"+
                "\n"+
                " Example usage:\n"+
                " --------------\n"+
                " help\n"+
                " help help";
        
        String exit = ""+
                " [exit,quit]\n"+
                "\n"+
                " Closes the program\n"+
                "\n"+
                " Example usage:\n"+
                " --------------\n"+
                " exit\n"+
                " quit";
        
        String gc = ""+
                " gc [stats]\n"+
                "\n"+
                " Either runs the garbage collector (to remove temporary files) or displays statistics about the currently tracked temporary files.\n"+
                "\n"+
                " Example Udage:\n"+
                " --------------\n"+
                " gc\n"+
                " gc stats";
        
        contents.put("select", select);
        contents.put("join", join);
        contents.put("where", where);
        contents.put("colstats", colstats);
        contents.put("help", help);
        contents.put("exit", exit);
        contents.put("gc", gc);
    }
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
    
    /**
     * A human readable list of entries in the help file.
     * @return 
     */
    private static String getHelpTOC()
    {
        String commands = "";
        boolean first = true;
        for(String command : contents.keySet())
        {
            if(!first)
                commands+=", ";
            first = false;
            commands+=command;
        }
        return commands;
    }
    
    /**
     * What to display when typing 'help' in the command line
     * @return 
     */
    public static String help()
    {
        
        return  ""+
                " Simple Hive v "+settings.version+"\n"+
                " ----------------------\n"+
                "\n"+
                " A SQL-like database app. For help, use help 'command' where command can be "
                + getHelpTOC();
        
    }
    
    /**
     * What to display when typing 'help theCommand' in the command line
     * @param theCommand
     * @return 
     */
    public static String help(String theCommand)
    {
        if(!contents.containsKey(theCommand.toLowerCase()))
        {
            return "Could not find entry for '"+theCommand+"'. Supported commands are "+getHelpTOC();
        }
        return contents.get(theCommand.toLowerCase());
    }
}
