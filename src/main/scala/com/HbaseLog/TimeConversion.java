package com.HbaseLog;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeConversion {

    public static void main(String[] args) throws Exception {

        String str = "1542442682048";
        Long time = Long.parseLong(str);
        Date date = new Date(time);
        Timestamp timestamp = new Timestamp(date.getTime());


        String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
        String pattern2 = "yyyyMMdd_HHmmss";
        SimpleDateFormat inputFormat = new SimpleDateFormat(pattern);
        SimpleDateFormat outputFormat = new SimpleDateFormat(pattern2);
        Date date1 = inputFormat.parse(timestamp.toString());
        outputFormat.format(date1);
        System.out.println(outputFormat.format(date1));
    }
}
