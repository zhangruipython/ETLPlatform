package com.application.stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author 张睿
 * @create 2020-05-20 13:52
 * rocksdb 数据生产者，传入kafka
 **/
public class RocksDbProducer {
    static {
        RocksDB.loadLibrary();
    }
    public static void main(String[] args) throws RocksDBException {
        String dbPath = "/home/hadoop/Documents/rocksdb-6.6.4/testdb02";
        Options options = new Options().setCreateIfMissing(true);
        String topic = "rocksdb01";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.54:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);
        try (RocksIterator iterator = RocksDB.open(options, dbPath).newIterator()) {
            byte[] seekKey = "t".getBytes(StandardCharsets.UTF_8);
            for (iterator.seek(seekKey); iterator.isValid(); iterator.next()) {
                if (new String(iterator.key(), StandardCharsets.UTF_8).startsWith("t")) {
                    ProducerRecord<String,String> record = new ProducerRecord<>(topic,new String(iterator.value(),StandardCharsets.UTF_8));
                    Future<RecordMetadata> future =  kafkaProducer.send(record);
                    System.out.println(future.get());
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}

