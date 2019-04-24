package com.bigdata.hadoop.mapreduce.reduce;

import com.bigdata.hadoop.mapreduce.model.UserDept;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;


public class ReduceJoinRuduce extends Reducer<Text,UserDept,NullWritable,UserDept> {
    private static final Logger logger = LoggerFactory.getLogger(ReduceJoinRuduce.class);

    @Override
    protected void reduce(Text key, Iterable<UserDept> values, Context context) throws IOException, InterruptedException {
        UserDept deptInfo =new UserDept() ;
        List<UserDept> userInfos = new ArrayList<>();
        for (UserDept userDept:values ) {
            if (userDept.getFlag()==1){
                UserDept userDeptinfo = new UserDept();
                //BeanUtils.copyProperties(userDeptinfo,userDept);
                userDeptinfo.setUserId(userDept.getUserId());
                userDeptinfo.setFlag(userDept.getFlag());
                userDeptinfo.setUsername(userDept.getUsername());
                userDeptinfo.setDeptId(userDept.getDeptId());
                userInfos.add(userDeptinfo);
            }else if(userDept.getFlag()==2){
                   // BeanUtils.copyProperties(deptInfo,userDept);
                deptInfo.setDeptName(userDept.getDeptName());
            }
        }
        for (UserDept userInfo:userInfos){
            userInfo.setDeptName(deptInfo.getDeptName());
            context.write(NullWritable.get(),userInfo);
        }

    }
}
