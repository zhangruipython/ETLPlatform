package com.application.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 张睿
 * @create 2020-06-04 16:32
 **/
public class demo {
    public void demo01() throws RocksDBException {
//        String dbPath = "D:\\data\\stateRocksdb";
//        CheckStructure checkStructure = new CheckStructure();
//        byte[] b1 = checkStructure.getOneValue(dbPath);
//        ValueDecode valueDecode = new ValueDecode();
//        byte[] value = valueDecode.decodeValue(b1);
//        System.out.println(value.length);
//        String mes = value.length<100?null:new String(value,StandardCharsets.UTF_8);
//        System.out.println(mes);
//        requireNonNull(mes,"反序列化后存在value值为0");
//        JSONObject jsonObject = JSONObject.parseObject(mes);
//        System.out.println(jsonObject.getString("hospitalizedNum"));
//        jsonObject.getString("hospitalizedNum");
//        JSONObject jsonObject = new JSONObject();
//        jsonObject.put("a",value);
//        JSONArray jsonArray = new JSONArray();
//        jsonArray.add(jsonObject);
//        return jsonArray.toJSONString();

    }
    public String demo02(String value){
        return value;
    }
    public void demo03(){
        List<String> list = Arrays.asList("a","b");
        Iterable<String> iterable = list;
        Iterator<String> iterator = iterable.iterator();
        while (iterator.hasNext()){
            System.out.println(iterator.next());
        }
//        System.out.println(iterable.iterator());
    }
    public void demo04(){
        AtomicLong count = new AtomicLong();
        int i =0 ;
        Thread thread = new Thread(){
            public void run(){

            }
        };
        
    }
    public void demo05(SparkSession sparkSession){
        List<String> data = Arrays.asList("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}",
                "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset<String> dataset = sparkSession.createDataset(data, Encoders.STRING());
        dataset.show();
        Dataset<Row> dataset1 = sparkSession.read().json(dataset);
        dataset1.show();
        long time01 = System.currentTimeMillis();
        System.out.println(dataset1.toJSON().collectAsList().toString());
        System.out.println(System.currentTimeMillis()-time01);
    }
    public void demo06(List<String> i){
        i.add("c");

    }
    public static void main(String[] args) throws RocksDBException {
//        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("demo");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        SparkSession spark = SparkSession.builder().appName("dataset demo").config(conf).getOrCreate();
        demo var = new demo();
        List<String> a = new ArrayList<>();
        a.add("a");
        var.demo06(a);
        System.out.println(a);
        System.out.println(Runtime.getRuntime().availableProcessors());


    }
}

