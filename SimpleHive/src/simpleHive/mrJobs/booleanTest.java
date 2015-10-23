/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHive.mrJobs;

import simpleHive.table;

/**
 *
 * @author toddbodnar
 */
public class booleanTest{
        boolean isVariable[];
        Object value[];
        String type;
        
        booleanTest subTests[] = null;
        boolean isAnd;
        /**
         * if no AND or OR in partial query, just test it, otherwise split and dive deeper
         * @param partialQuery 
         */
        public booleanTest(String partialQuery)
        {
            //System.out.println("Building paritalQuery");
            //System.out.println(partialQuery);
            if(!partialQuery.toLowerCase().contains(" and ")&&!partialQuery.toLowerCase().contains(" or "))
            {
                //if at a leaf
                isVariable = new boolean[]{false,false};
                value = new Object[]{null,null};
                String split[] = partialQuery.split(" ");
                for(int ct=0;ct<3;ct+=2) //test first and last part of split
                {
                    if(split[ct].startsWith("_col"))
                    {
                        isVariable[ct/2]=true;
                        value[ct/2] = Integer.parseInt(split[ct].substring(4));
                    }
                    else
                    {
                        isVariable[ct/2]=false;
                        value[ct/2] = split[ct];
                    }
                }
                type = split[1];
                
            }
            else
            {
                if(partialQuery.toLowerCase().contains(" or "))
                {
                    int split = 0;
                    for(int ct=0;ct<partialQuery.length();ct++)
                    {
                        if(partialQuery.toLowerCase().substring(ct).startsWith(" or "))
                        {
                            split = ct;
                            break;
                        }
                    }
                    subTests = new booleanTest[]{null,null};
                    isAnd = false;
                    subTests[0] = new booleanTest(partialQuery.substring(0, split));
                    subTests[1] = new booleanTest(partialQuery.substring(split+4));
                }
                else
                {
                    int split = 0;
                    for(int ct=0;ct<partialQuery.length();ct++)
                    {
                        if(partialQuery.toLowerCase().substring(ct).startsWith(" and "))
                        {
                            split = ct;
                            break;
                        }
                    }
                    subTests = new booleanTest[]{null,null};
                    isAnd = true;
                    subTests[0] = new booleanTest(partialQuery.substring(0, split));
                    subTests[1] = new booleanTest(partialQuery.substring(split+5));
                }
            }
            
        }
        public boolean evaluate(table t) throws Exception
        {
            return evaluate(t.get());
        }
        /**
         * is the current row in the given table valid?
         * @param row
         * @return 
         */
        public boolean evaluate(Object row[]) throws Exception
        {
            if(subTests==null)
            {
                Object elements[] = new Object[2];
                for(int ct=0;ct<2;ct++)
                {
                    if(isVariable[ct])
                        elements[ct] = row[((int) value[ct])];
                    else
                        elements[ct] = value[ct];
                                
                }
                switch(type)
                {
                    case "=":
                        return elements[0].toString().equals(elements[1].toString());
                    case "!=":
                        return !elements[0].toString().equals(elements[1].toString());
                    case "<":
                        return Double.parseDouble(elements[0].toString()) < Double.parseDouble(elements[1].toString());
                    case "<=":
                        return Double.parseDouble(elements[0].toString()) <= Double.parseDouble(elements[1].toString());
                    case ">":
                        return Double.parseDouble(elements[0].toString()) > Double.parseDouble(elements[1].toString());
                    case ">=":
                        return Double.parseDouble(elements[0].toString()) >= Double.parseDouble(elements[1].toString());
                }
                
                throw new Exception("Cannot evaluate comparator "+type);
            }
            else
            {
                if(isAnd)
                    return subTests[0].evaluate(row) && subTests[1].evaluate(row);
                else
                    return subTests[0].evaluate(row) || subTests[1].evaluate(row);
            }
        }
    }
