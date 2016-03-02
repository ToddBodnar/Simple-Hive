/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

import java.util.ArrayList;
import java.util.LinkedList;
import com.toddbodnar.simpleHive.IO.ramFile;
import com.toddbodnar.simpleHadoop.simpleContext;
import com.toddbodnar.simpleHive.metastore.table;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author toddbodnar
 */
public class leftJoin extends query<Text, Text> {

    /**
     * Joins two tables
     *
     * @param key the location of the key in the table set by setInput
     * @param otherkey the location of the key in the table, other
     */
    public leftJoin(int key, int otherKey) {
        mainKey = key;
        this.otherKey = otherKey;
    }

    public void setOtherInput(table other) {
        this.other = other;
    }

    public table getOtherInput() {
        return other;
    }

    table other;
    ramFile storage;
    int mainKey, otherKey;

    @Override
    public table getOutput() {

        if (super.getOutput() != null) {
            return super.getOutput();
        }

        String names[] = new String[getInput().getColNames().length + getOtherInput().getColNames().length];
        for (int ct = 0; ct < getInput().getColNames().length; ct++) {
            names[ct] = getInput().getColName(ct);
        }
        for (int ct = 0; ct < other.getColNames().length; ct++) {
            names[getInput().getColNames().length + ct] = other.getColName(ct);
        }

        table result = new table(new ramFile(), names);
        result.setSeperator(getInput().getSeperator());
        super.setOutput(result);
        return result;

    }

    @Override
    public Reducer getReducer() {

        return new LeftJoinReducer();
    }

    private static class LeftJoinReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context cont) throws IOException, InterruptedException {
            String left, right;
            left = right = null;
            for (Text input : values) {
                if (input.toString().charAt(0) == '0') {
                    left = input.toString().substring(1);
                } else {
                    right = input.toString().substring(1);
                }
            }
            if (left == null || right == null)//only join if both tables have a matching key
            {
                return;
            }

            cont.write(new Text(left + "\0" + right), null);

        }
    }

    private static class LeftJoinMapperCombined extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text line, Mapper.Context cont) throws IOException, InterruptedException {

            int tableId = ((IntWritable[]) key)[0].get();

            if (tableId == 1) {
                cont.write(new Text(line.toString().split(cont.getConfiguration().get("SIMPLE_HIVE.JOIN.INPUT_SEPERATOR.1"))[cont.getConfiguration().getInt("SIMPLE_HIVE.JOIN.KEY.1", -1)]), new Text('0' + line.toString()));
            } else if (tableId == 2) {
                cont.write(new Text(line.toString().split(cont.getConfiguration().get("SIMPLE_HIVE.JOIN.INPUT_SEPERATOR.2"))[cont.getConfiguration().getInt("SIMPLE_HIVE.JOIN.KEY.2", -1)]), new Text('1' + line.toString()));

            } else {
                throw new IOException("Invalid table number, expected 1 or 2, got " + tableId);
            }
        }
    }
    
    private static class LeftJoinMapperCombinedOne extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text line, Mapper.Context cont) throws IOException, InterruptedException {

            
            
                cont.write(new Text(line.toString().split(cont.getConfiguration().get("SIMPLE_HIVE.JOIN.INPUT_SEPERATOR.1"))[cont.getConfiguration().getInt("SIMPLE_HIVE.JOIN.KEY.1", -1)]), new Text('0' + line.toString()));
            
        }
    }
    
    private static class LeftJoinMapperCombinedTwo extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text line, Mapper.Context cont) throws IOException, InterruptedException {

           
            
                cont.write(new Text(line.toString().split(cont.getConfiguration().get("SIMPLE_HIVE.JOIN.INPUT_SEPERATOR.2"))[cont.getConfiguration().getInt("SIMPLE_HIVE.JOIN.KEY.2", -1)]), new Text('1' + line.toString()));
            
        }
    }

    @Override
    public Mapper getMapper() {
        return new LeftJoinMapperCombined();
    }
    
    public Mapper[] getMapperPairs()
    {
        return new Mapper[]{new LeftJoinMapperCombinedOne(),new LeftJoinMapperCombinedTwo()};
    }

    @Override
    public Class getKeyType() {
        return Text.class;
    }

    @Override
    public Class getValueType() {
        return Text.class;
    }

    @Override
    public void writeConfig(Configuration conf) {
        conf.set("SIMPLE_HIVE.JOIN.INPUT_SEPERATOR.1", getInput().getSeperator());
        conf.setInt("SIMPLE_HIVE.JOIN.KEY.1", mainKey);

        conf.set("SIMPLE_HIVE.JOIN.INPUT_SEPERATOR.2", getOtherInput().getSeperator());
        conf.setInt("SIMPLE_HIVE.JOIN.KEY.2", otherKey);
    }

}
