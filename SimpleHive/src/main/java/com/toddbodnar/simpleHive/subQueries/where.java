/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.toddbodnar.simpleHive.IO.file;
import com.toddbodnar.simpleHive.IO.ramFile;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHadoop.MapReduceJob;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author toddbodnar
 */
public class where extends query<Text, NullWritable> {

    booleanTest theQuery;
    String partialQuery, parsedQuery;

    /**
     * given a partial query (the part after the where) of the form: _col1 <
     * "string" AND _col2 = "string" AND _col3 = "string"
     *
     * @param partialQuery
     */
    public where(String partialQuery) {
        this.partialQuery = partialQuery;

    }
    
    public String toString()
    {
        return "Where query :"+partialQuery+". "+((parsedQuery==null)?"Not yet parsed":("Compiled to "+parsedQuery));
    }

    public void parseInput() {

        //replace the variables in the query string with the actual column numbers
        String split[] = partialQuery.split(" ");
        for (int ct = 0; ct < split.length; ct++) {
            int colNum = getInput().getColNum(split[ct]);
            if (colNum != -1) {
                split[ct] = "_col" + colNum;
            }
        }
        parsedQuery = split[0];
        for (int ct = 1; ct < split.length; ct++) {
            parsedQuery += " " + split[ct];
        }

        //theQuery = new booleanTest(partialQuery);
    }

    @Override
    public Reducer getReducer() {
        return new WhereReducer();
    }

    private static class WhereReducer extends Reducer {
    ;//just default behavior, since all processing is done map side

    }

    @Override
    public Mapper<Object, Text, Text, NullWritable> getMapper() {
        return new WhereMapper();
    }

    private static class WhereMapper extends Mapper<Object, Text, Text, NullWritable> {

        private booleanTest theQuery;

        public void setup(Context cont) {
            Logger.getGlobal().warning("Where "+cont.getConfiguration().get("SIMPLE_HIVE.WHERE.QUERY"));
            theQuery = new booleanTest(cont.getConfiguration().get("SIMPLE_HIVE.WHERE.QUERY"));
        }

        public void map(Object key, Text line, Mapper.Context cont) throws IOException, InterruptedException {
            try {
                if (theQuery.evaluate((Object[]) line.toString().split(cont.getConfiguration().get("SIMPLE_HIVE.WHERE.INPUT_SEPERATOR")))) {
                    cont.write(line, NullWritable.get());
                }
            } catch (Exception ex) {
                Logger.getLogger(where.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    @Override
    public Class getKeyType() {
        return Text.class;
    }

    @Override
    public Class getValueType() {
        return NullWritable.class;
    }

    public void setInput(table in) {
        super.setInput(in);
        parseInput();
    }

    public table getOutput() {
        if (super.getOutput() != null) {
            return super.getOutput();
        }
        table result = new table(new ramFile(), getInput().getColNames());
        result.setSeperator(getInput().getSeperator());
        super.setOutput(result);
        return super.getOutput();
    }

    @Override
    public void writeConfig(Configuration conf) {
        conf.set("SIMPLE_HIVE.WHERE.QUERY", parsedQuery);
        conf.set("SIMPLE_HIVE.WHERE.INPUT_SEPERATOR", getInput().getSeperator());
    }

}
