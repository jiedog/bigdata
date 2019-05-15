package com.bigdata.spark.util;

import jdk.internal.dynalink.beans.StaticClass;
import scala.runtime.Statics;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 *
 */
public class DateUtil {

    public static String parse(String timestr) throws Exception{
        DateFormat sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH);
        DateFormat targetFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String time = timestr.substring(1,timestr.length()-1);
        String reusult = targetFormat.format(sourceFormat.parse(time));
        return reusult;
    }
}
