/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tfm.utad.reducerdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducerPigKey implements WritableComparable, Serializable {

    private Long key;
    private Double latitude;
    private Double longitude;
    private String userid;
    private Date date;
    private String activity;

    private final SimpleDateFormat sdf = new SimpleDateFormat(ReducerConstants.FORMATTER_DATE_YEAR_MONTH_DATE_HOURS_MINUTES_SECONDS);
    private final static Logger LOG = LoggerFactory.getLogger(ReducerPigKey.class);

    public ReducerPigKey() {

    }

    public ReducerPigKey(Long key, Double latitude, Double longitude, String userID, Date date, String activity) {
        this.key = key;
        this.latitude = latitude;
        this.longitude = longitude;
        this.userid = userID;
        this.date = date;
        this.activity = activity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(key);
        out.writeDouble(latitude);
        out.writeDouble(longitude);
        out.writeUTF(userid);
        out.writeUTF(sdf.format(date));
        out.writeUTF(activity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key = in.readLong();
        latitude = in.readDouble();
        longitude = in.readDouble();
        userid = in.readUTF();
        try {
            date = sdf.parse(in.readUTF());
        } catch (ParseException ex) {
            LOG.error("Error parsing date: " + ex.toString());
            date = null;
        }
        activity = in.readUTF();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.key);
        hash = 79 * hash + Objects.hashCode(this.latitude);
        hash = 79 * hash + Objects.hashCode(this.longitude);
        hash = 79 * hash + Objects.hashCode(this.userid);
        hash = 79 * hash + Objects.hashCode(this.date);
        hash = 79 * hash + Objects.hashCode(this.activity);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ReducerPigKey other = (ReducerPigKey) obj;
        if (!Objects.equals(this.key, other.key)) {
            return false;
        }
        if (!Objects.equals(this.latitude, other.latitude)) {
            return false;
        }
        if (!Objects.equals(this.longitude, other.longitude)) {
            return false;
        }
        if (!Objects.equals(this.userid, other.userid)) {
            return false;
        }
        if (!Objects.equals(this.date, other.date)) {
            return false;
        }
        return Objects.equals(this.activity, other.activity);
    }

    @Override
    public String toString() {
        return key + ReducerConstants.SPLIT_SEMICOLON + latitude + ReducerConstants.SPLIT_SEMICOLON + longitude + ReducerConstants.SPLIT_SEMICOLON + userid + ReducerConstants.SPLIT_SEMICOLON + sdf.format(date) + ReducerConstants.SPLIT_SEMICOLON + activity;
    }

    @Override
    public int compareTo(Object o) {
        ReducerPigKey other = (ReducerPigKey) o;
        return this.key.compareTo(other.key);
    }
}