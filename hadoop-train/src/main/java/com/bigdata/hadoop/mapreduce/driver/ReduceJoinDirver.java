package com.bigdata.hadoop.mapreduce.driver;

import com.bigdata.hadoop.mapreduce.mapper.ReduceJoinMapper;
import com.bigdata.hadoop.mapreduce.model.UserDept;
import com.bigdata.hadoop.mapreduce.reduce.ReduceJoinRuduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author 种庆凯
 * @Date ${date} ${time}
 */
public class ReduceJoinDirver {
    private static final Logger logger = LoggerFactory.getLogger(ReduceJoinDirver.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length!=2){
            System.err.print("请输入2个参数  input  output");
            System.exit(1);
        }
        String input = args[0];//hadoop-train/join/input
        String ouput = args[1];//hadoop-train/join/output/
        Configuration configuration =new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outPutPath = new Path(ouput);
        // /hadoop-train/output/d=20190324 already exists
        //判断文件是否存在，存在需要删除
        if(fileSystem.exists(outPutPath)){
            fileSystem.delete(outPutPath,true);
        }
        logger.info("Processing trade with value: {}  ", ouput);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ReduceJoinDirver.class);
        job.setMapperClass(ReduceJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserDept.class);
        FileInputFormat.setInputPaths(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(ouput));
        job.setReducerClass(ReduceJoinRuduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }

}
