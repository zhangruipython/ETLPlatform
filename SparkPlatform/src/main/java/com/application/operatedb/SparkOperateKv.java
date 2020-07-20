package com.application.operatedb;

/**
 * @author 张睿
 * @create 2020-04-28 9:34
 * 读取rocksdb数据库数据转化为dataset，执行SQL操作
 **/

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.util.ValueDecode;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class SparkOperateKv {
    private final SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("demo");
    private final JavaSparkContext sc = new JavaSparkContext(conf);
    private final SparkSession spark = SparkSession.builder().appName("dataset demo").config(conf).getOrCreate();
    private static final String DB_PATH = "/home/hadoop/Documents/rocksdb-6.6.4/stateRocksdb";
//    private static final String DB_PATH = "D:/data/stateRocksdb";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    Options options = new Options().setCreateIfMissing(true);
    static {
        RocksDB.loadLibrary();
    }

    /**
     * 原始方法
     *  数据提取，转化 生成data frame
    * @author 张睿
    * @date 2020-06-22
    * @param columnNames metadata 指定生成列名
    * @return java.util.List<byte[]>
    */

    public Dataset<Row> createDataFrame(List<String> columnNames) throws RocksDBException {
        try (RocksDB rocksDB = RocksDB.openReadOnly(options,DB_PATH);RocksIterator iterator = rocksDB.newIterator()){
            List<byte[]> valueByteList = new LinkedList<>();
            ValueDecode valueDecode = new ValueDecode();
            // 解析value 返回json对象
            for (iterator.seekToFirst();iterator.isValid();iterator.next()){
                valueByteList.add(iterator.value());
            }
            List<JSONObject> valueJsonList = valueByteList.parallelStream()
                    .map(s-> new String(valueDecode.decodeValue(s),CHARSET))
                    .filter(s -> s.startsWith("{"))
                    .map(JSONObject::parseObject)
                    .collect(Collectors.toList());

            // 定义data frame格式
            List<StructField> fields = columnNames.stream().map(record->DataTypes.createStructField(record,DataTypes.StringType,true)).collect(Collectors.toList());
            StructType schema = DataTypes.createStructType(fields);
            // 将json对象集合转为 rdd
            JavaRDD<JSONObject> valuesRdd = sc.parallelize(valueJsonList);

            JavaRDD<Row> rowJavaRDD = valuesRdd.map((Function<JSONObject, Row>) record->{
                List<String> attributes = new ArrayList<>();
                for(String column:columnNames){
                    String var03 = record.getString(column);
                    attributes.add(var03);
                }
                Object[] objects = attributes.toArray(new Object[0]);
                return RowFactory.create(objects);
            });
            return spark.createDataFrame(rowJavaRDD,schema);
        }
    }
    /**rocksdb表中value数据格式为json类型，将json字符串直接转为data frame */
    // TODO: 使用spark的原生json字符串转为 data frame 速度较慢
    public Dataset<Row> createDataFrame01() throws RocksDBException {
        try (RocksDB rocksDB = RocksDB.openReadOnly(options, DB_PATH); RocksIterator iterator = rocksDB.newIterator()) {
            List<String> valueJsonList = new ArrayList<>();
            ValueDecode valueDecode = new ValueDecode();
            // 解析value 返回json字符串集合
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] var01 = valueDecode.decodeValue(iterator.value());
                String var02 = var01.length != 0 ? new String(var01, CHARSET) : null;
                if (var02 != null && var02.startsWith("{")) {
                    try {
                        valueJsonList.add(var02);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            Dataset<String> dataset = spark.createDataset(valueJsonList, Encoders.STRING());
            return spark.read().json(dataset);
        }
    }

    public String executeSql(String sqlContent,String tableName,List<String> columns) throws RocksDBException {
        Dataset<Row> dataFrameRocks = createDataFrame(columns);
        // TODO:添加缓存前总耗时：14003ms 添加缓存后12990ms
        //  取出数据后并行化处理耗时 12539ms
        dataFrameRocks.cache();
        dataFrameRocks.show(10);
        dataFrameRocks.createOrReplaceTempView(tableName);
        spark.catalog().cacheTable(tableName);
        // TODO:DataFrame转为json字符串这一步执行速度过慢，可能需要手动序列化，跳过spark DAG
        return spark.sql(sqlContent).toJSON().collectAsList().toString();
    }
    /**读取200万数据，转为spark dataFrame，执行sql操作*/
    public void transformRdd() throws RocksDBException {
        try (RocksIterator iterator = RocksDB.open(options,DB_PATH).newIterator()){
            byte[] seekKey = "t".getBytes(CHARSET);
            // 检索==>遍历==>插入
            long time01 = System.currentTimeMillis();
            List<byte[]> kvList = new LinkedList<>();
            for (iterator.seek(seekKey); iterator.isValid();iterator.next()){
                if (new String(iterator.key(),CHARSET).startsWith("t")){
                    kvList.add(iterator.value());
                }
            }
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
            Dataset<Row> lineDf = spark.createDataFrame(rowRdd,schema);
            lineDf.createOrReplaceTempView("employees");
            //这一段java误认异常实际没问题
            long time02 = System.currentTimeMillis();
            Dataset<Row> result = spark.sql("SELECT count(gender) FROM employees GROUP BY gender");
            result.show();
            System.out.println(System.currentTimeMillis()-time02);
            System.out.println("执行sql耗时");
        }
    }


    public static void main(String[] args) throws RocksDBException {
        if (args.length==3){
            org.example.operatespark.SparkOperateKv sparkOperateKv = new org.example.operatespark.SparkOperateKv();
            long tim01 = System.currentTimeMillis();
//            String sqlContent = "select * from table01 limit 10";
//            String tableName = "table01";
//            List<String> columns = Arrays.asList("hospitalizedNum","hid","treatEndDate","treatBeginDate","insuranceType","__MODEL_NAME","credentialType","medicalType",
//                    "credentialNum","TABLE_NAME","MAIN_ID","name","authorSerialNumber","insuranceId","HS_TABLE_DATA","txId","txTime","idKey","indexRList","searchValues");
            String sqlContent = args[0];
            String tableName = args[1];
            List<String> columns = Arrays.asList(args[2].split(","));
            String result = sparkOperateKv.executeSql(sqlContent,tableName,columns);
            System.out.println(result);
            System.out.println(System.currentTimeMillis()-tim01);
            System.out.println("all use time");
        }
    }
}

