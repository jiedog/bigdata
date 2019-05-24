package com.bigdata.spark.etl;


import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

import java.io.Serializable;

/**
 * @Author 种庆凯
 * @Date ${date} ${time}
 */
public class EtlApp  implements scala.Serializable{


    public static String etl(String[] x){
        String cdn = x[0];
        String region = x[1];
        String level = x[2];
        String time = x[3];
        //var time = timestr.substring(1,timestr.length()-1);
        //val time = DateUtil.parse(timestr)
        String ip = x[4];
        String domain = x[5];
        String url = x[6];
        String traffic = x[7];
        StringBuilder stringBuilder = new StringBuilder("");
        stringBuilder.append(cdn).append("\t")
                .append(region).append("\t")
                .append(level).append("\t")
                .append(time).append("\t")
                .append(ip).append("\t")
                .append(domain).append("\t")
                .append(url).append("\t")
                .append(traffic);
        String result=stringBuilder.toString();

        return result;
    }

}
