package com.application.operatedb;

/**
 * @author 张睿
 * @create 2020-04-28 9:34
 * 读取rocksdb数据库数据转化为dataset，执行SQL操作
 **/

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class SparkOperateKv {
    public static final String DB_PATH = "/home/hadoop/Documents/rocksdb-6.6.4/testdb02";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    Options options = new Options().setCreateIfMissing(true);

    static {
        RocksDB.loadLibrary();
    }


    /**直接遍历kv数据，通过定义java bean写入dataset*/
    public void prefixIterKeys(SparkSession spark) throws RocksDBException {
        try (RocksIterator iterator = RocksDB.open(options,DB_PATH).newIterator()){
            byte[] seekKey = "t".getBytes(CHARSET);
            RocksData rocksData = new RocksData();
            Encoder<RocksData> dataEncoder = Encoders.bean(RocksData.class);
            List<RocksData> dataBeans = new ArrayList<RocksData>();
            long splitTime = System.currentTimeMillis();
            for (iterator.seek(seekKey); iterator.isValid();iterator.next()){
                if (new String(iterator.key(),CHARSET).startsWith("t")){
                    String[] values = new String(iterator.value(),CHARSET).split("@#");
                    rocksData.setName(values[0]);
                    rocksData.setGender(values[1]);
                    rocksData.setSaleAmount(values[2]);
                    rocksData.setEvent(values[3]);
                    rocksData.setAge(values[4]);
                    rocksData.setShopTime(values[5]);
                    dataBeans.add(rocksData);
                }
            }
            System.out.println("split time"+(System.currentTimeMillis()-splitTime));
            // 这一步出现Java 堆OM
            Dataset<RocksData> beanDs = spark.createDataset(dataBeans,dataEncoder);
            beanDs.show();
        }
    }

    public void readAvro(SparkSession spark){
        Dataset<Row> usersDf = spark.read().format("avro").load("D:/MyProject/SparkPlatform/src/main/java/com/application/users.avro");
        usersDf.show();
    }
    /**读取200万数据，转为spark dataFrame，执行sql操作
     * 共*/
    public void transformRdd(JavaSparkContext sc, SparkSession spark) throws RocksDBException {
        try (RocksIterator iterator = RocksDB.open(options,DB_PATH).newIterator()){
            byte[] seekKey = "t".getBytes(CHARSET);
            // 检索器（可以并行优化）
            List<byte[]> kvList = new ArrayList<>();
            for (iterator.seek(seekKey); iterator.isValid();iterator.next()){
                if (new String(iterator.key(),CHARSET).startsWith("t")){
                    kvList.add(iterator.value());
                }
            }
            // 将value值写入rdd，对value值做split操作
            JavaRDD<byte[]> linesRdd = sc.parallelize(kvList);
            // 将rdd转化为dataframe
            // 读取字段
            String schemaString = "name gender sale_amount event age shop_time";
            List<StructField> fields = new ArrayList<>();
            String a = " ";
            for (String fieldName : schemaString.split(a)) {
                StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);
            // 切分value值，转为row
            JavaRDD<Row> rowRdd = linesRdd.map((Function<byte[], Row>) record->{
                String[] attributes  = new String(record,CHARSET).split("@#");
                return RowFactory.create(attributes[0],attributes[1],attributes[2],attributes[3],attributes[4],attributes[5]);
            });
            Dataset<Row> lineDf = spark.createDataFrame(rowRdd,schema);
            lineDf.createOrReplaceTempView("employees");
            //这一段java误认异常实际没问题
            long time01 = System.currentTimeMillis();
            Dataset<Row> result = spark.sql("SELECT * FROM employees WHERE age=25");
            System.out.println("sql use time"+(System.currentTimeMillis()-time01)/1000);
            result.show();
        }
    }
    /**rocksdb 数据转为 java对象
     * Stream耗时 2s
     * parallelStream耗时 1s*/
    public void transformKv() throws RocksDBException{
        try (RocksIterator iterator = RocksDB.open(options,DB_PATH).newIterator()){
            byte[] seekKey = "t".getBytes(CHARSET);
            List<byte[]> kvList = new LinkedList<>();
            for (iterator.seek(seekKey); iterator.isValid();iterator.next()){
                if (new String(iterator.key(),CHARSET).startsWith("t")){
                    kvList.add(iterator.value());
                }
            }
            RocksData rocksData = new RocksData();
            List<RocksData> rocksDataList = kvList.parallelStream().map(l->{
                String[] valueList = new String(l,CHARSET).split("@#");
                rocksData.setName(valueList[0]);
                rocksData.setGender(valueList[1]);
                rocksData.setSaleAmount(valueList[2]);
                rocksData.setEvent(valueList[3]);
                rocksData.setAge(valueList[4]);
                rocksData.setShopTime(valueList[5]);
                return rocksData;
            }).collect(Collectors.toList());
        }

    }

    public static void main(String[] args) throws RocksDBException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("dataset demo").config(conf).getOrCreate();
        SparkOperateKv sparkOperateKv = new SparkOperateKv();
        long tim01 = System.currentTimeMillis();
        sparkOperateKv.transformRdd(sc,spark);
        System.out.println("use time "+(System.currentTimeMillis()-tim01)/1000);
        spark.stop();
    }
}

