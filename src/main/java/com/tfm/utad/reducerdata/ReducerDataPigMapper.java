/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.tfm.utad.reducerdata;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducerDataPigMapper extends
        Mapper<Object, Text, LongWritable, ReducerPigKey> {
 
    private static SimpleDateFormat sdf;
    
    private final static Logger LOG = LoggerFactory.getLogger(ReducerDataPigMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        sdf = new SimpleDateFormat(ReducerConstants.FORMATTER_DATE_YEAR_MONTH_DATE_HOURS_MINUTES_SECONDS);
    }
 
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] values = value.toString().replaceAll("\n", "").split(ReducerConstants.SPLIT_SEMICOLON);
        Double latitude = Double.valueOf(values[0]);
        Double longitude = Double.valueOf(values[1]);
        String userid = values[2];
        Date date;
        try {
            date = sdf.parse(values[3]);
        } catch (ParseException ex) {
            LOG.error("Error parsing date: " + ex.toString());
            date = null;
        }
        String activity = values[4];
        if (isValidKey(latitude, longitude, date)) {
            Text text = (Text)key;
            Long id = Long.valueOf(text.toString());
            context.write(new LongWritable(id), new ReducerPigKey(id, latitude, longitude, userid, date, activity));
        } else {
            LOG.error("Invalid values in line: " + value.toString());
            context.getCounter(ReducerDataEnum.MALFORMED_DATA).increment(1);
        }
    }
    
    private boolean isValidKey(Double latitude, Double longitude, Date date) {
        return latitude != null && longitude != null && date != null;
    }
}