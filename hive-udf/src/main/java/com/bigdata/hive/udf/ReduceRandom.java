package com.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * @Author 种庆凯
 * @Date ${date} ${time}
 */
public class ReduceRandom extends UDF {
    public String evaluate(String input){
        String output=input.substring(input.indexOf("_")+1);
        return output;
    }
    public static void main(String[] args) {
        ReduceRandom reduceRandom =new ReduceRandom();
        String output = reduceRandom.evaluate("9_abc");
        System.out.println(
                output
        );
    }
}
