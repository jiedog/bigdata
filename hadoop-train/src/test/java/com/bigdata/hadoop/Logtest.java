package com.bigdata.hadoop;

import com.bigdata.hadoop.mapreduce.driver.LogETLDirver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author 种庆凯
 * @Date ${date} ${time}
 */
public class Logtest {
    private static final Logger logger = LoggerFactory.getLogger(Logtest.class);

    public static void main(String[] args) {
       logger.info("id:{},name:{}",22,"aaa");
    }
}
