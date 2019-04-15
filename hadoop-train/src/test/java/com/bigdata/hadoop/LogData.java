package com.bigdata.hadoop;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 * @Author 种庆凯
 * @Date ${date} ${time}
 */
public class LogData {
    private LogData logData;

    @Before
    public void setUp() throws Exception {
        logData = new LogData();
    }

    @After
    public void tearDown() throws Exception {
        logData = null;
    }

    /**
     * baidu	CN	E	[17/Jul/2018:17:07:50 +0800]	223.104.18.110	v2.go2yd.com	http://v1.go2yd.com/user_upload/1531633977627104fdecdc68fe7a2c4b96b2226fd3f4c.mp4_bd.mp4	16384
     *
     * @param
     */

    public String createData() {
        String result = "";
        Random random = new Random();
        int x = random.nextInt(10);
        String[] times = new String[]{
                "[24/Mar/2019:08:07:50 +0800]",
                "[24/Mar/2019:09:08:50 +0800]",
                "[24/Mar/2019:10:17:50 +0800]",
                "[24/Mar/2019:11:27:50 +0800]",
                "[24/Mar/2019:12:37:50 +0800]",
                "[24/Mar/2019:13:47:50 +0800]",
                "[24/Mar/2019:14:57:50 +0800]",
                "[24/Mar/2019:15:07:50 +0800]",
                "[24/Mar/2019:18:17:50 +0800]",
                "[24/Mar/2019:20:27:50 +0800]"
        };

        String[] urls = new String[]{
                "v1.go1yd.com",
                "v2.go2yd.com",
                "v3.go3yd.com",
                "v4.go4yd.com",
                "v5.go5yd.com",
                "v6.go6yd.com",
                "v7.go7yd.com",
                "v8.go8yd.com",
                "v9.go9yd.com",
                "v10.go10yd.com"
        };
        String[] ips = new String[]{
                "223.104.18.110",
                "223.104.18.10",
                "223.104.18.11",
                "223.104.28.110",
                "223.104.28.11",
                "223.104.28.10",
                "223.104.28.55",
                "223.173.18.110",
                "213.173.28.110",
                "203.173.28.110",
        };
        String cdn = "baidu";
        String region = "CN";
        String level = "E";
        String time = times[x];
        String ip = ips[x];
        //http://v1.go2yd.com/user_upload/1531633977627104fdecdc68fe7a2c4b96b2226fd3f4c.mp4_bd.mp4
        String domain = "http://" + urls[x] + "/user_upload/" + System.currentTimeMillis() + random.nextInt(1000000) + ".mp4";
        String url = urls[x];
        String traffic = String.valueOf(random.nextInt(20000));
        StringBuilder stringBuilder = new StringBuilder("");
        stringBuilder.append(cdn).append("\t")
                .append(region).append("\t")
                .append(level).append("\t")
                .append(time).append("\t")
                .append(ip).append("\t")
                .append(domain).append("\t")
                .append(url).append("\t")
                .append(traffic);
        result = stringBuilder.toString();
        return result;

    }

    @Test
    public void write() {
        FileWriter fw = null;
        StringBuilder stringBuilder = new StringBuilder("");
        for (int i = 1; i < 1000000; i++) {
            stringBuilder.append(logData.createData()).append("\n");
        }

        // System.out.println(stringBuilder.toString());

        try {
            File f = new File("baidu.log");
            fw = new FileWriter(f, true);

        } catch (IOException e) {
            e.printStackTrace();
        }
        PrintWriter pw = new PrintWriter(fw);
        pw.println(stringBuilder.toString());
        pw.flush();
        try {
            fw.flush();
            pw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
