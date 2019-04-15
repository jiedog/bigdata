package com.bigdata.hadoop.utils;

import java.text.ParseException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * @author: cqk
*/
public class LogUtils {
    DateFormat sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH);
    DateFormat targetFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    public String parse(String log){
        String result="";

        try{
            String[] splits=log.split("\t");
            String cdn = splits[0];
            String region = splits[1];
            String level = splits[2];
            String timestr = splits[3];
            String time = timestr.substring(1,timestr.length()-1);
            time = targetFormat.format(sourceFormat.parse(time));
            String ip = splits[4];
            String domain = splits[5];
            String url = splits[6];
            String traffic = splits[7];
            StringBuilder stringBuilder = new StringBuilder("");
            stringBuilder.append(cdn).append("\t")
                    .append(region).append("\t")
                    .append(level).append("\t")
                    .append(time).append("\t")
                    .append(ip).append("\t")
                    .append(domain).append("\t")
                    .append(url).append("\t")
                    .append(traffic);
            result=stringBuilder.toString();
        }catch (ParseException e){
            e.printStackTrace();
        }

        return log;
    }
}

