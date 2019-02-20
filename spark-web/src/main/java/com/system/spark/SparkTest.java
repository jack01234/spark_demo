package com.system.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkTest {
    public static void main(String[] args) {
        String logFile = "D:/软件/spark-2.4.0-bin-hadoop2.7/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().master("local").appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();

        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> stringJavaRDD = sc.textFile(logFile);
        JavaRDD<Integer> map = stringJavaRDD.map(s -> s.length());
        Integer reduce = map.reduce((a, b) -> a + b);
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+reduce);
    }
}
