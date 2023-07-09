package com.loda.util;

import javolution.io.Struct;

/**
 * @Author loda
 * @Date 2023/3/8 20:17
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Ip2DecimalV2 {
    public static void main(String[] args) {
        String ip = "255.24.78.129";
        String[] splits = ip.split("\\.");

        Double resDec = Double.parseDouble(splits[0])* Math.pow(256,3) +
                Double.parseDouble(splits[1]) * Math.pow(256,2) +
                Double.parseDouble(splits[2]) * Math.pow(256,1) +
                Double.parseDouble(splits[3]) * Math.pow(256,0);
        System.out.println(resDec);

        //在255左移移24位后，因为int是有符号位，造成溢出
        long res = (long) Integer.parseInt(splits[0]) << 24 |
                (long) Integer.parseInt(splits[1]) << 16 |
                (long) Integer.parseInt(splits[2]) << 8 |
                (long)Integer.parseInt(splits[3]);
        System.out.println(res);


    }
}
