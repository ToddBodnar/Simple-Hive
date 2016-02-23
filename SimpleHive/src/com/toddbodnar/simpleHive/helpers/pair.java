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
public class pair <key,value>{
    private key theKey;
    private value theValue;
    public pair(key k, value v)
    {
        theKey=k;
        theValue=v;
    }
    public key getKey()
    {
        return theKey;
    }
    public value getValue()
    {
        return theValue;
    }
}
