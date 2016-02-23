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
     * @param other the other table to join
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

        return new Reducer<Text, Text, Text, Text>() {
            @Override
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
        };
    }

    @Override
    public Mapper getMapper() {
        return new Mapper<IntWritable[], Text, Text, Text>() {

            @Override
            public void map(IntWritable key[], Text line, Mapper.Context cont) throws IOException, InterruptedException {

                int tableId = key[0].get();

                if (tableId == 1) {
                    cont.write(new Text(line.toString().split(getInput().getSeperator())[mainKey]), new Text('0' + line.toString()));
                } else if (tableId == 2) {
                    cont.write(new Text(line.toString().split(getOtherInput().getSeperator())[otherKey]), new Text('1' + line.toString()));

                } else {
                    throw new IOException("Invalid table number, expected 1 or 2, got " + tableId);
                }
            }
        };
    }

    @Override
    public Class getKeyType() {
        return Text.class;
    }

    @Override
    public Class getValueType() {
        return Text.class;
    }

}
