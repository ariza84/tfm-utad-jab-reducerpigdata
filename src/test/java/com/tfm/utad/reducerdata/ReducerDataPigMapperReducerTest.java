/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tfm.utad.reducerdata;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class ReducerDataPigMapperReducerTest {

    private MapDriver<Object, Text, LongWritable, ReducerPigKey> mapDriver;
    private ReduceDriver<LongWritable, ReducerPigKey, Text, NullWritable> reduceDriver;
    private MapReduceDriver<Object, Text, LongWritable, ReducerPigKey, Text, NullWritable> mapReduceDriver;
    private SimpleDateFormat sdf;

    @Before
    public void setUp() {
        ReducerDataPigMapper mapper = new ReducerDataPigMapper();
        ReducerDataPigReducer reducer = new ReducerDataPigReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        sdf = new SimpleDateFormat(ReducerConstants.FORMATTER_DATE_YEAR_MONTH_DATE_HOURS_MINUTES_SECONDS);
    }

    @Test
    public void testMapper() throws IOException, ParseException {
        String key = "123456";
        mapDriver.withInput(new Text(key), new Text(
                "40.48989;-3.65754;User189;2014-12-06 17:43:21;20141206-34567-189"));
        Date date = sdf.parse("2014-12-06 17:43:21");
        mapDriver.withOutput(new LongWritable(new Long(key)), new ReducerPigKey(new Long(key), Double.valueOf("40.48989"), Double.valueOf("-3.65754"), "User189", date, "20141206-34567-189"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException, ParseException {
        List<ReducerPigKey> values = new ArrayList<>();
        Date date = sdf.parse("2014-12-06 17:43:21");
        ReducerPigKey pigKey = new ReducerPigKey(new Long("123456"), Double.valueOf("40.48989"), Double.valueOf("-3.65754"), "User189", date, "20141206-34567-189");
        values.add(pigKey);
        reduceDriver.withInput(new LongWritable(new Long("123456")), values);
        reduceDriver.withOutput(new Text(pigKey.toString()), NullWritable.get());
        reduceDriver.runTest();
    }
}