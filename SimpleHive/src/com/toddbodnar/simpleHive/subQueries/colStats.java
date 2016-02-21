/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.toddbodnar.simpleHive.subQueries;

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
public class colStats extends query<DoubleWritable, Text> {

    public colStats(int statsCol) {
        this(statsCol, -1);
    }

    /**
     * Constructs a stats job for column statsCol, optimally grouped by groupBy
     *
     * @param statsCol
     * @param groupBy
     */
    public colStats(int statsCol, int groupBy) {
        groupByCol = groupBy;
        this.statsCol = statsCol;

    }

    public void reduce(Object key, LinkedList values) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0;
        double ct = 0;

        for (Object value : values) {
            double number = Double.parseDouble((String) value);
            if (min > number) {
                min = number;
            }
            if (max < number) {
                max = number;
            }
            sum += number;
            ct++;
        }

        double avg = sum / ct;

        String result = max + "\0" + min + "\0" + avg + "\0" + sum + "\0" + ct + "\0" + key;

        resultStorage.append(result);
    }

    private int groupByCol, statsCol;
    private ramFile resultStorage;
    private static String stats[] = new String[]{"group", "max", "min", "avg", "sum", "count"};

    @Override
    public table getOutput() {
        if (super.getOutput() != null) {
            return super.getOutput();
        }
        table result = new table(new ramFile(), stats);
        result.setSeperator(getInput().getSeperator());
        super.setOutput(result);
        return result;
    }

    @Override
    public Mapper getMapper() {
        return new Mapper<IntWritable, Text, Text, DoubleWritable>() {

            public void map(IntWritable key, Text line, Mapper.Context cont) throws IOException, InterruptedException {

                String row[] = line.toString().split(getInput().getSeperator());
                String groupKey = "*";
                if (groupByCol != -1) {
                    groupKey = row[groupByCol].toString();
                }

                cont.write(new Text(groupKey), new DoubleWritable(Double.parseDouble(row[statsCol])));
            }
        };
    }

    @Override
    public Reducer getReducer() {

        return new Reducer<Text, DoubleWritable, Text, Text>() {
            public void reduce(Text key, Iterable<DoubleWritable> values, Context cont) throws IOException, InterruptedException {
                double min = Double.MAX_VALUE;
                double max = Double.MIN_VALUE;
                double sum = 0;
                double ct = 0;

                for (DoubleWritable value : values) {
                    double number = value.get();
                    if (min > number) {
                        min = number;
                    }
                    if (max < number) {
                        max = number;
                    }
                    sum += number;
                    ct++;
                }

                double avg = sum / ct;

                String result = max + "\0" + min + "\0" + avg + "\0" + sum + "\0" + ct;
                cont.write(key, new Text(result));
            }
        };

    }

    @Override
    public Class getKeyType() {
        return Text.class;
    }

    @Override
    public Class getValueType() {
        return DoubleWritable.class;
    }
}
