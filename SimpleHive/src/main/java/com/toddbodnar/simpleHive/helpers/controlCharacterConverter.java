/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.helpers;

import org.apache.commons.lang.NotImplementedException;

/**
 *
 * @author toddbodnar
 */
public class controlCharacterConverter {
    public static String convertToReadable(String c)
    {
        if(c.length()>1)
            throw new NotImplementedException("Not implemented multi-character seperators yet");
        if((int)c.charAt(0) > 15)
            return c;
        return ("CONTROL_"+((int)c.charAt(0)));
    }
    
    public static String convertFromReadable(String c)
    {
        if(!c.startsWith("CONTROL_"))
            return c;
        int controlCharNumber = Integer.parseInt(c.substring(8));
        return (char)controlCharNumber+"";
    }
            
}
