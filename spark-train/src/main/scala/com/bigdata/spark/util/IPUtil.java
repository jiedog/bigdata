package com.bigdata.spark.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpURL;
import org.apache.http.HttpEntity;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


public class IPUtil {
    private   String url = "http://ip.taobao.com/service/getIpInfo.php?ip=";
    public static String getCity(String ip) throws Exception{
        URL url = new URL("http://ip.taobao.com/service/getIpInfo.php?ip="+ip);
        HttpURLConnection httpUrl = (HttpURLConnection)url.openConnection();
        InputStream is = httpUrl.getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is,"utf-8"));
        String line;
        StringBuilder sb = new StringBuilder();
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        is.close();
        br.close();

        //获得网页源码
        return sb.toString().trim();
    }

    public static void main(String[] args) throws Exception{
        String ip="223.104.18.110";
        String info= getCity(ip);
        JSONObject jsStr = JSONObject.parseObject(info);
        JSONObject data = JSONObject.parseObject(jsStr.get("data").toString());

        System.out.println(data.get("region"));
    }
}
