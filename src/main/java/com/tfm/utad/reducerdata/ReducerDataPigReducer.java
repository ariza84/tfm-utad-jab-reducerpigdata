/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.tfm.utad.reducerdata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerDataPigReducer extends
        Reducer<LongWritable, ReducerPigKey, Text, NullWritable> {
    
    @Override
    public void reduce(LongWritable key, Iterable<ReducerPigKey> values, Context context)
            throws IOException, InterruptedException {
        for (ReducerPigKey value : values) {
            String str = value.toString().replace("\n", "");
            context.write(new Text(str), NullWritable.get());
        }
    }
}