package com.application.stream;

import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * @author 张睿
 * @create 2020-05-18 17:25
 **/
public class StreamSocketClient {
    public static void main(String[] args) throws StreamingQueryException {
        // 出现
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setMaster("local[*]");
        SparkSession spark = SparkSession.builder().appName("StructuredStreaming").config(conf).getOrCreate();
        Dataset<Row> lines = spark.readStream().format("socket").option("host","localhost").option("port","9999").load();
        // 写入内存表中
        StreamingQuery query = lines.groupBy("value").count().writeStream().outputMode(OutputMode.Append()).format("console").start();
        query.awaitTermination();
        spark.close();
    }
}

