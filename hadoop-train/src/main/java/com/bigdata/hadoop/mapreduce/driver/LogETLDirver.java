package com.bigdata.hadoop.mapreduce.driver;


import com.bigdata.hadoop.mapreduce.mapper.LogETLMapper;
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

/**
 * @Author cqk
 */
public class LogETLDirver {
    private static final Logger logger = LoggerFactory.getLogger(LogETLDirver.class);
    public static void main(String[] args) throws Exception{
        if(args.length!=2){
            System.err.print("请输入2个参数  input  output");
            System.exit(1);
        }
        String input = args[0];//hadoop-train/input/
        String ouput = args[1];//hadoop-train/output/d=20190324
        Configuration configuration =new Configuration();

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outPutPath = new Path(ouput);
        //判断文件是否存在，存在需要删除
        if(fileSystem.exists(outPutPath)){
            fileSystem.delete(outPutPath,true);
        }
        logger.info("Processing trade with value: {}  ", ouput);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(LogETLDirver.class);
        job.setMapperClass(LogETLMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(ouput));
        //FileOutputFormat.setCompressOutput(job, true);
       // FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.waitForCompletion(true);
    }
}

