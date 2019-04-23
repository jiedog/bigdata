package com.bigdata.hadoop.mapreduce.mapper;

import com.bigdata.hadoop.mapreduce.model.UserDept;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 */
public class ReduceJoinMapper extends Mapper<LongWritable,Text,Text ,UserDept>  {
    private static final Logger logger = LoggerFactory.getLogger(ReduceJoinMapper.class);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] info = value.toString().split("\t");
        Integer flag ;
        String dataVlue= "" ;
        String dataKey="";
        UserDept userDept = new UserDept();
        if (info.length==3){//user
            flag = 1;
            String userId = info[0];
            String username = info[1];
            String deptId = info[2];
            dataKey = deptId;
            userDept.setFlag(flag);
            userDept.setUserId(userId);
            userDept.setUsername(username);
            userDept.setDeptId(deptId);
        }else if(info.length==2){
            flag =2;
            String deptId = info[0];
            String deptName = info[1];
            dataKey=deptId;
            userDept.setFlag(flag);
            userDept.setDeptName(deptName);
        }
        context.write(new Text(dataKey), userDept);


    }
}
