package com.application.stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author 张睿
 * @create 2020-05-19 16:31
 * spark structured streaming 作为kafka数据处理客户端
 **/
public class StructuredKafka {
    public static void main(String[] args) throws StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("org").setLevel(Level.WARN);
        SparkConf sc = new SparkConf().setMaster("local[*]");
        SparkSession spark = SparkSession.builder().appName("demo").config(sc).getOrCreate();
        Dataset<DeviceData> lines = spark.readStream().format("kafka").option("kafka.bootstrap.servers","192.168.1.54:9092")
                .option("subscribe","rocksdb01").load().as(ExpressionEncoder.javaBean(DeviceData.class));
        Dataset<Row> lineDf = lines.selectExpr("CAST(value AS STRING)");
        // 拆分value
        List<String> schemaList =  Arrays.asList("name","gender","sale_amount","event","age","shop_time");
        Column column = functions.col("value");
        Column linesSplit = functions.split(column,"@#");
        for(int i=0;i<schemaList.size();i++){
            lineDf = lineDf.withColumn(schemaList.get(i),linesSplit.getItem(i));
        }

        lineDf.drop("value").createOrReplaceTempView("tmp01");

        /**Append追加模式适用于非聚合操作，如果要在Append模式中使用聚合，需要使用水印进行聚合*/

        Dataset<Row> lineSql = spark.sqlContext().sql("select * from tmp01 where age>20");

        // foreachBatch 对每个批进行处理，保存每个批次的查询结果为list<Row>
        List<Row> rows = new ArrayList<>();
        StreamingQuery query2 = lineSql.writeStream().outputMode(OutputMode.Append()).foreachBatch(
                new VoidFunction2<Dataset<Row>, Long>() {
                    @Override
                    public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                        System.out.println("dataframe handle start");
                        List<Row> a =rowDataset.toJavaRDD().collect();
                        rows.addAll(a);
                        System.out.println(rows.size());
                        System.out.println("dataframe handle stop");
                    }
                }
        ).start();
        query2.awaitTermination();
    }
}

