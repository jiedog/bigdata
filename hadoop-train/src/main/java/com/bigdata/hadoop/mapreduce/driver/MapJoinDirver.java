package com.bigdata.hadoop.mapreduce.driver;

import com.bigdata.hadoop.mapreduce.mapper.MapJoinMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * mapjoin dirver
 */
public class MapJoinDirver {
    private static final Logger logger = LoggerFactory.getLogger(MapJoinDirver.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length!=3){
            System.err.print("请输入3个参数  input1 input2 output");
            System.exit(1);
        }
        String input1 = args[0];//hadoop-train/join/input/user.txt
        String input2 = args[1];//hadoop-train/join/input/dept.txt
        String ouput = args[2];//hadoop-train/join/output/
        Configuration configuration =new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outPutPath = new Path(ouput);

        //判断文件是否存在，存在需要删除
        if(fileSystem.exists(outPutPath)){
            fileSystem.delete(outPutPath,true);
        }
        Job job = Job.getInstance(configuration);
        job.addCacheFile(new Path(input2).toUri());
        job.setJarByClass(MapJoinDirver.class);
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path(input1));
        FileOutputFormat.setOutputPath(job,new Path(ouput));
        job.waitForCompletion(true);
    }

}
