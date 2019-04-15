package com.bigdata.hadoop.mapreduce.format;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * @Author 种庆凯
 * @Date ${date} ${time}
 */
public class MyInputFormat extends FileInputFormat<LongWritable,Text> {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        String delimiter   ="\t";
        byte[] recordReaderBytes = null;
        recordReaderBytes=delimiter.getBytes(Charsets.UTF_8);
        return new LineRecordReader(recordReaderBytes);
    }

}
