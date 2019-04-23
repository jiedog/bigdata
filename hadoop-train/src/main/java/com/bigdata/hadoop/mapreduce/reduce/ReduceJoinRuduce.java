package com.bigdata.hadoop.mapreduce.reduce;

import com.bigdata.hadoop.mapreduce.mapper.ReduceJoinMapper;
import com.bigdata.hadoop.mapreduce.model.UserDept;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author 种庆凯
 * @Date ${date} ${time}
 */
public class ReduceJoinRuduce extends Reducer<Text,UserDept,NullWritable,Text> {
    private static final Logger logger = LoggerFactory.getLogger(ReduceJoinRuduce.class);

    @Override
    protected void reduce(Text key, Iterable<UserDept> values, Context context) throws IOException, InterruptedException {
        UserDept deptInfo = new UserDept();
        List<UserDept> userInfos = new ArrayList<>();
        for (UserDept userDept:values ) {
            if (userDept.getFlag()==1){
                userInfos.add(userDept);
            }else if(userDept.getFlag()==2){
                deptInfo = userDept;
            }
        }
        for (UserDept userInfo:userInfos){
            userInfo.setDeptName(deptInfo.getDeptName());
        }
        context.write(NullWritable.get(),new Text(userInfos.toString()));
    }
}
