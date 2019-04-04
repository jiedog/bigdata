package com.bigdata.hadoop.mapreduce.mapper;

import com.bigdata.hadoop.utils.LogUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: cqk
*/
public class LogETLMapper extends Mapper<LongWritable,Text,NullWritable,Text> {
    /**
     * map进行数据清晰
     * 一条数据清洗完后进行输出
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int length=value.toString().split("\t").length;
        if(length==8){
            LogUtils utils = new LogUtils();
            String result=utils.parse(value.toString());
            if(StringUtils.isNotBlank(result)){
                context.write(NullWritable.get(),new Text(result));
            }
        }
    }
}

