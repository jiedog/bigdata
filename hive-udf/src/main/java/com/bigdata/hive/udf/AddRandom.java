package com.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Random;

/**
 * @Author 种庆凯
 * @Date ${date} ${time}
 */
public class AddRandom extends UDF {

    public String evaluate(String input){
        int random = new Random().nextInt(10);
        return random+"_"+input;
    }

    public static void main(String[] args) {
        AddRandom addRandom =new AddRandom();
        String output = addRandom.evaluate("abc");
        System.out.println(
                output
        );
    }
}
