package com.bigdata.hadoop.mapreduce.mapper;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * map端join实现
 *dept 为小表 进行缓存
 */
public class MapJoinMapper extends Mapper<LongWritable,Text,Text,Text>{
    private static final Logger logger = LoggerFactory.getLogger(MapJoinMapper.class);
    private Map<String, String> map = new HashMap<String, String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 预处理把要关联的文件加载到缓存中来 创建BufferReader去读取小表
        BufferedReader reader = new BufferedReader(new FileReader(context.getCacheFiles()[0].toString()));
        String str = null;
        try {
            // 一行一行读取
            while ((str = reader.readLine()) != null){
                // 对缓存中小表(dept表)的数据进行切割
                String[] splits = str.split(",");
                map.put(splits[0],splits[1]);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            reader.close();
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] info = value.toString().split(",");
        // 获取从HDFS中加载的大表(user表)
        String[] userinfo = value.toString().split(",");
        String userId = userinfo[0];
        String username=userinfo[1];
        String deptId = userinfo[2];
        // 根据从内存中的关联表中获取要关联的属性deptName
        String deptName = map.get(deptId);
        // 写出去
        context.write(new Text(deptId), new Text(username + " " + deptName+" "+userId));
    }




}
