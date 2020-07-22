package com.application.operatedb;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

/**
 * @author 张睿
 * @create 2020-07-06 13:16
 * 测试由spark sql 生成双表join操作耗时
 **/
public class SparkTableJoin {
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    Options options = new Options().setCreateIfMissing(true);

    static {
        RocksDB.loadLibrary();
    }
    public Dataset<Row> transformRdd(JavaSparkContext sc, SparkSession spark,String dbPath) throws RocksDBException {
        try (RocksIterator iterator = RocksDB.open(options,dbPath).newIterator()){
            byte[] seekKey = "t".getBytes(CHARSET);
            // 检索==>遍历==>插入
            long time01 = System.currentTimeMillis();
            List<byte[]> kvList = new LinkedList<>();
            for (iterator.seek(seekKey); iterator.isValid();iterator.next()){
                if (new String(iterator.key(),CHARSET).startsWith("t")){
                    kvList.add(iterator.value());
                }
            }

            System.out.println(System.currentTimeMillis()-time01);
            System.out.println("遍历数据插入数据结构耗时");
            // 将value值写入rdd，对value值做split操作
            JavaRDD<byte[]> linesRdd = sc.parallelize(kvList);
            // 将rdd转化为data frame
            // 读取字段
            String schemaString = "name gender sale_amount event age shop_time";
            List<StructField> fields = new LinkedList<>();
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
            return spark.createDataFrame(rowRdd,schema);
        }
    }

    public static void main(String[] args) throws RocksDBException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("dataset demo").config(conf).getOrCreate();

        SparkTableJoin sparkTableJoin = new SparkTableJoin();
        String db01 = "/home/hadoop/Documents/rocksdb-6.6.4/testdb02";
        String db02 = "/home/hadoop/Documents/rocksdb-6.6.4/testdb03";
        Dataset<Row> db01Df = sparkTableJoin.transformRdd(sc,spark,db01);
        Dataset<Row> db02Df = sparkTableJoin.transformRdd(sc,spark,db02);
        db01Df.createOrReplaceTempView("db01");
        db02Df.createOrReplaceTempView("db02");
        long time01 = System.currentTimeMillis();
        Dataset<Row> result = spark.sql("set spark.sql.cbo=true;select db01.gender,db01.name,db02.event from db01 join db02 on db01.name=db02.name;");
        result.show();
        System.out.println("execute sql use time "+(System.currentTimeMillis()-time01));
    }
}

